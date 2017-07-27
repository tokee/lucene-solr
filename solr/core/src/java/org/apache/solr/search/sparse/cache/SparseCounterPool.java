/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search.sparse.cache;

import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.sparse.SparseKeys;
import org.apache.solr.search.sparse.count.DualPlaneMutable;
import org.apache.solr.search.sparse.count.PackedOpportunistic;
import org.apache.solr.search.sparse.track.SparseNPlane;
import org.apache.solr.search.sparse.track.SparseCounterInt;
import org.apache.solr.search.sparse.track.SparseCounterPacked;
import org.apache.solr.search.sparse.track.SparseCounterThreaded;
import org.apache.solr.search.sparse.track.ValueCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains a pool of SparseCounters, taking care of allocation, used counter clearing and re-use.
 * <br/>
 * Motivation: Constantly allocating large counter structures taxes the garbage collector of the JVM. It is faster to
 * clear & re-use structures instead. This is especially true for sparse structures, where the clearing time is
 * proportional to the number of previously updated counters.
 * <br/>
 * Motivation 2: With multi-shard setups, faceting is very often multi-phased, with phase 2 being fine-counting of
 * specific terms in the facet. If the pool holds a filled (aka non-cleared) counter from phase 1,   
 * <br/>
 * The pool and/or the content of the pool is bound to the index. When the index is updated and a new facet
 * request is issued, the pool will be freed and a new one will be created.
 * <br/>
 * The pool has a fixed maximum limit. If there are no cleaned counters available when requested, a new one
 * will be created. If the pool has reached maximum size and a counter is released, the counter is de-referenced,
 * removing it from the heap. It is advisable to set the pool size to a bit more than the number of concurrent
 * faceted searches, to avoid new allocations.
 * <br/>
 * Setting the pool size very high has the adverse effect of a constant heap overhead, dictated by the maximum
 * number of concurrent facet requests encountered since last index (re)open.
 * <br/>
 * The pool consists of a mix of empty (ready for any use) and filled (cached for re-use) counters. If the amount
 * of empty counters gets below the stated threshold, filled counters are cleaned and inserted af empty.
 * <br/>
 * Default behaviour for the pool is to perform counter clearing in 1 background thread, as this makes it possible
 * to provide a facet result to the caller faster. This can be controlled with {@link #setCleaningThreads(int)}.
 * <br/>
 * This class is thread safe and with no heavy synchronized parts.
 */
// TODO: Make it possible to specify max pool size as memory
// TODO: Add time-based clearing of filled counters, under the assumption that the common use case is phase 2 speed-up
// TODO: Make it possible to block on counter request (here be deadlock-dragons)
@SuppressWarnings("NullableProblems")
public class SparseCounterPool {
  public static Logger log = LoggerFactory.getLogger(SparseCounterPool.class);

  /**
   * The pool contains a mix of empty and filled ValueCounters. Filled counters at the beginning, empty counters
   * at the end of the linked list. When the pool-setup changes or a ValueCounter is removed or added, a janitor
   * job is requested. A janitor attempts to ensure that the pool is in the desired state, but is limited to one
   * cleanup-action before it returns, to avoid thread starvation in the thread pool.
   */
  private final LinkedList<ValueCounter> pool;

  /**
   * Holds a template for future instantiations of counters.
   * This is currently only used by counters relying on NPlaneMutable.
   */
  private ValueCounter template = null;

  /**
   * The maximum amount of ValueCounters to keep in the pool.
   * <br/>
   * If the setup is single-shard, the pool might not be fully filled if the background clearer is faster than
   * the overall request rate. If the setup is multi-shard, there is a higher chance of the pool being filled
   * as it will also contain filled counters, intended for re-use.
   */
  private int maxPoolSize;
  /**
   * The minimum amount of empty counters that should be in the pool. This parameter is only relevant for multi-shard
   * setups and controls the balance between filled counters for re-use in phase 2 of faceting and empty counters
   * ready for new use.
   */
  private int minEmptyCounters;
  /**
   * The amount of counter clears currently running in the background.
   */
  private final AtomicInteger activeClears = new AtomicInteger(0);
  /**
   * A String indentifying the layout of the ValueCounters handled by this SparseCounterPool.
   */
  private String structureKey = null;

  // The following properties are inherent to the searcher and should not change over the life of the pool once updated.
  // maxCountForAny is a core property as it dictates both size & correctness of packed value counters.
  protected final String field;
  protected final String description;
  private long maxCountForAny = -1;
  private long referenceCount = -1;
  private int maxDoc = -1;
  private int uniqueValues = -1;
  private boolean initialized = false;

  private final List<TimeStat> timeStats = new ArrayList<>(); // Keeps track of all TimeStats and is used for debug & clears
  private final List<NumStat> numStats = new ArrayList<>();   // Keeps track of all NumStats and is used for clears

  // Performance statistics
  private final TimeStat requestClears = new TimeStat("requestClear");
  private final TimeStat backgroundClears = new TimeStat("backgroundClear");

  // TODO: Clean this up with a HashMap or similar, so that the implementations are not tightly coupled to the pool
  private final TimeStat packedAllocations = new TimeStat("packedAllocation");
  private final TimeStat nplaneAllocations = new TimeStat("nplaneAllocation");
  private final TimeStat intAllocations = new TimeStat("intAllocation");
  private final TimeStat dualPlaneAllocations = new TimeStat("dualPlaneAllocation");

  private final TimeStat collections = new TimeStat("collect");
  private final TimeStat extractions = new TimeStat("extract");
  private final TimeStat resolvings = new TimeStat("resolve");
  private final TimeStat simpleFacetTotal = new TimeStat("simpleFacetTotal");
  private final TimeStat termsListTotal = new TimeStat("termsListFacetTotal");
  private final TimeStat termsListLookup = new TimeStat("termsListLookup");
  private final TimeStat termLookup = new TimeStat("termLookup", 1);
  private final NumStat termLookupMissing = new NumStat("termLookupMissing");
  public final TimeStat regexpMatches = new TimeStat("regexpChecks", 1);

  // Pool cleaning stats
  private NumStat emptyReuses = new NumStat("emptyReuses");
  private NumStat filledReuses = new NumStat("filledReuses");
  private NumStat filledFrees = new NumStat("filledFrees");
  private NumStat emptyFrees = new NumStat("emptyFrees");

  // Misc. stats
  NumStat disables = new NumStat("disables");
  NumStat withinCutoffCount = new NumStat("withinCutoff");
  NumStat exceededCutoffCount = new NumStat("exceededCutoff");

  String lastTermsListRequest = "N/A";
  String lastTermLookup = "N/A";

  NumStat cacheHits = new NumStat("cacheHits");
  NumStat cacheMisses = new NumStat("cacheMisses");

  /**
   * The supervisor is shared between all pools under the same searcher.
   * It normally has a single thread available for background cleaning.
   */
  protected final ThreadPoolExecutor supervisor;
  public static final String NEEDS_CLEANING = "DIRTY";

  // Cached terms for fast ordinal lookup
  // TODO: Consider removing this - it is a very specialized and potentially very heavy cache
  private BytesRefArray externalTerms = null;
  // Optionally defined histogram of the number of counters with the given maxBits
  // TODO: This should be handled by the template
  private long[] histogram = null;
  // Optionally defined histogram of the number of counters with the given bits needed by {@link NPlaneMutable}
  // for setups with fast tracking on non-zero counters.
  // TODO: This should be handled by the template
  private long[] plusOneHistogram = null;

  /**
   * Core setup. {@link #setFieldProperties(int, long, int, long)} must be called before the pool can deliver counters.
   * @param janitorSupervisor shared executor for cache clean up.
   * @param field             the Solr field that is faceted on.
   * @param description
   * @param maxPoolSize       the maximum pool size, measures in ValueCounter instances.
   * @param minEmptyCounters  the minimum amount of empty counters that the pool should strive to hold.
   */
  public SparseCounterPool(ThreadPoolExecutor janitorSupervisor, String field, String description,
                           int maxPoolSize, int minEmptyCounters) {
    this.field = field;
    this.description = description;
    supervisor = janitorSupervisor;
    this.maxPoolSize = maxPoolSize;
    this.minEmptyCounters = minEmptyCounters;
    pool =  new LinkedList<>();
  }

  /**
   * The field properties determine the layout of the sparse counters and is part of the sparse/non-sparse estimation.
   * This method must be called before calling {@link #acquire}.  It should only be called once.
   * @param uniqueValues   the number of unique values in the field. This number must be specified and must be correct.
   * @param maxCountForAny the maximum value that any individual counter can reach.  If this value is not determined,
   *                       -1 should be used. If -1 is specified, this number can be updated at a later time.
   * @param maxDoc         the maxCount from the searcher.  Must be specified, should be correct.
   * @param references     the number of references from documents to terms in the facet field.  Must be specified,
   *                       should be correct.
   */
  public synchronized void setFieldProperties(int uniqueValues, long maxCountForAny, int maxDoc, long references) {
    if (initialized) {
      log.warn("setFieldProperties has already been called for field '" + field + "' and should only be called once");
    }
    this.uniqueValues = uniqueValues;
    this.maxCountForAny = maxCountForAny;
    this.maxDoc = maxDoc;
    this.referenceCount = references;
    initialized = true;
  }

  /**
   * Delivers a counter ready for updates. The type of counter will be chosen based on uniqueTerms, maxCountForAny and
   * the general Sparse setup from sparseKeys. This is the recommended way to get sparse counters.
   * <br/>
   * Note: It is possible that this returns null. This can only happen for implementation == nplane.
   *       In that case, it is the responsibility of the caller to use {@link #addAndReturn instead}.
   * @param sparseKeys     setup for the Sparse system as well as the specific call.
   * @param implementation the counter implementation to acquire.
   * @return a counter ready for updates or null if a counter could not be created.
   */
  public ValueCounter acquire(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation) {
    ValueCounter vc = getCounter(sparseKeys, implementation);
    if (vc == null) { // Got nothing, so we allocate a new one
      ValueCounter newCounter = createCounter(sparseKeys, implementation);
      if (template == null) {
        template = newCounter;
      }
      return newCounter;
    }

    if (sparseKeys.cacheToken == null) {
      if (vc.getContentKey() == null) { // Asked for empty, got empty
        emptyReuses.inc();
        return vc;
      }
      // Asked for empty, got filled
      final long clearTime = System.nanoTime();
      vc.clear();
      requestClears.incRel(clearTime);
      return vc;
    }

    if (vc.getContentKey() == null) { // Asked for filled, got empty
      emptyReuses.inc();
      cacheMisses.inc();
      return vc;
    } else if (sparseKeys.cacheToken.equals(vc.getContentKey())) { // Asked for filled, got match

      cacheHits.inc();
      return vc;
    }
    // Asked for filled, got wrong filled (might just be a NEEDS_CLEANING)
    cacheMisses.inc();
    final long clearTime = System.nanoTime();
    vc.clear();
    requestClears.incRel(clearTime);
    return vc;
  }

  /**
   * Provides the pool with a new counter created from the outside, registers the counter in the system and returns it.
   *
   * @param sparseKeys setup for the Sparse system as well as the specific call.
   * @param implementation the implementation of the counter to add.
   * @param newVC the counter to add.
   * @param allocateStartTime marker set just before collecting needed statistics and allocating the counter.
   * @return the given counter.
   */
  public ValueCounter addAndReturn(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation, ValueCounter newVC,
                                   long allocateStartTime) {
    // Checking for existing and potential reset of the structureKey and pool
    ValueCounter vc = getCounter(sparseKeys, implementation);
    if (vc != null) {
      log.info("addAndReturn: An available counter " + vc.getClass().getSimpleName()
          + " already existed, but was discarded");
    }
    template = newVC;
    switch (implementation) {
      case array:
        intAllocations.incRel(allocateStartTime);
        break;
      case packed:
        packedAllocations.incRel(allocateStartTime);
        break;
      case dualplane:
        dualPlaneAllocations.incRel(allocateStartTime);
        break;
      case nplane:
        nplaneAllocations.incRel(allocateStartTime);
        break;
      case nplanez:
        nplaneAllocations.incRel(allocateStartTime);
        break;
      default: throw new UnsupportedOperationException(
          "Unable to add statistics for counter implementation " + implementation);
    }
    return newVC;
  }

  private String createStructureKey(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation) {
    switch (implementation) {
      case array:
        return SparseCounterInt.createStructureKey(
            uniqueValues, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction);
      case packed:
        return sparseKeys.countingThreads == 1 ?
            SparseCounterPacked.createStructureKey(
                uniqueValues, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked) :
            SparseCounterThreaded.createStructureKey(
                uniqueValues, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked,
                sparseKeys.counter);
      case dualplane:
      case nplane: // TODO: Add split/spank/max etc
        return SparseCounterThreaded.createStructureKey(
            uniqueValues, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked,
            sparseKeys.counter);
      case nplanez:
        return SparseNPlane.createStructureKey(
            uniqueValues, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.counter);
      default: throw new UnsupportedOperationException(
          "Counter implementation '" + implementation + "' is not supported");
    }
  }

  public boolean usePacked(SparseKeys sparseKeys) {
    // TODO: Add support for the other counters
    return sparseKeys.counter != SparseKeys.COUNTER_IMPL.array &&
        (((maxCountForAny != -1 && PackedInts.bitsRequired(maxCountForAny) <= sparseKeys.packedLimit)) ||
            maxCountForAny > Integer.MAX_VALUE);
  }

  private ValueCounter createCounter(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation) {
    final long allocateTime = System.nanoTime();
    ValueCounter vc;
    switch (implementation) {
      case array: {
        vc = new SparseCounterInt(
            uniqueValues, maxCountForAny == -1 ? Integer.MAX_VALUE : maxCountForAny, sparseKeys.minTags,
            sparseKeys.fraction, sparseKeys.maxCountsTracked);
        intAllocations.incRel(allocateTime);
        break;
      }
      case packed: {
        if (sparseKeys.countingThreads == 1) {
          vc = new SparseCounterPacked(
              uniqueValues, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
        } else {
          PackedInts.Mutable counts = PackedOpportunistic.create(uniqueValues, PackedInts.bitsRequired(maxCountForAny));
          vc = new SparseCounterThreaded(implementation, counts, maxCountForAny, sparseKeys.minTags,
              sparseKeys.fraction, sparseKeys.maxCountsTracked);
        }
        packedAllocations.incRel(allocateTime);
        break;
      }
      case dualplane: {
        if (getHistogram() == null) {
          throw new IllegalStateException(
              "Attempted to allocate dualplane counter, but the required histogram did not exist");
        }
        PackedInts.Mutable counts = DualPlaneMutable.create(getHistogram());
        vc = new SparseCounterThreaded(implementation, counts, maxCountForAny, sparseKeys.minTags,
            sparseKeys.fraction, sparseKeys.maxCountsTracked);
        dualPlaneAllocations.incRel(allocateTime);
        break;
      }
      case nplane: {
        synchronized (pool) {
          // nplane needs a template in the form of an already existing nplane
          if (template == null) {
            return null; // The caller needs to create the counter from scratch
          }
          if (!(template instanceof SparseCounterThreaded)) {
            throw new IllegalStateException(
                "Logic error: nplane counting was requested and an a template existed, but the template was a "
                    + template.getClass().getSimpleName() + " where " + SparseCounterThreaded.class.getSimpleName()
                    + " was required");
          }
          SparseCounterThreaded scp = (SparseCounterThreaded)template;
          if (scp.counterImpl != implementation) {
            throw new IllegalStateException(
                "Logic error: nplane counting was requested and an a template existed, but the template contained a "
                    + "counter with implementation " + scp.counterImpl);
          }

          ValueCounter counter = scp.createSibling();
          nplaneAllocations.incRel(allocateTime);
          return counter;
        }
      }
      case nplanez: {
        synchronized (pool) {
          // nplane needs a template in the form of an already existing nplane
          if (template == null) {
            return null; // The caller needs to create the counter from scratch
          }
          if (!(template instanceof SparseNPlane)) {
            throw new IllegalStateException(
                "Logic error: nplanez counting was requested and an a template existed, but the template was a "
                    + template.getClass().getSimpleName() + " where " + SparseNPlane.class.getSimpleName()
                    + " was required");
          }
          SparseNPlane scp = (SparseNPlane)template;
          if (scp.counterImpl != implementation) {
            throw new IllegalStateException(
                "Logic error: nplanez counting was requested and an a template existed, but the template contained a "
                    + "counter with implementation " + scp.counterImpl);
          }

          ValueCounter counter = scp.createSibling();
          nplaneAllocations.incRel(allocateTime);
          return counter;
        }
      }

      default: throw new UnsupportedOperationException(
          "Counter implementation '" + implementation + "' is not supported");
    }
    return vc;
  }


  /**
   * Release a counter after use. This method will return immediately.
   * <br/>
   * @param counter a used counter.
   * @param sparseKeys the facet keys associated with the counter.
   */
  public void release(ValueCounter counter, SparseKeys sparseKeys) {

    if (counter.explicitlyDisabled()) {
      disables.inc();
    }
    if (structureKey != null && !counter.getStructureKey().equals(structureKey)) {
      // Setup changed, cannot use the counter anymore
      filledFrees.inc();
      return;
    }
    if (counter.getContentKey() != null) {
      // If the ValueCounter already has a content key, it means that the call resulting in this release must be
      // distributed faceting phase 2 and thus that the values are not needed anymore
      counter.setContentKey(NEEDS_CLEANING);
    } else {
      // Assign a contentKey to mark the counter as filled.
      // If the cacheToken is present, the counter with content will be cached for an expected phase 2 call.
      counter.setContentKey(sparseKeys.cacheToken == null ? NEEDS_CLEANING : sparseKeys.cacheToken);
    }
    synchronized (pool) {
      pool.add(counter);
      if (template == null) {
        template = counter;
      }
    }
    triggerJanitor();
  }

  /**
   * Called by the background cleaner when a counter has been processed.
   * @param counter a fully cleaned counter, ready for use.
   */
  private void releaseCleared(ValueCounter counter) {
    synchronized (pool) {
      if ((structureKey != null && !counter.getStructureKey().equals(structureKey)) || pool.size() >= maxPoolSize) {
        // Setup changed or pool full. Skip insert!
        emptyFrees.inc();
        return;
      }
      pool.add(counter);
      if (template == null) {
        template = counter;
      }
      structureKey = counter.getStructureKey();
    }
  }

  /**
   * Calculates if a faceting on the field with the given number of hits should be expected to fall within sparse tracking limits.
   * @param hitCount     the number of hits.
   * @param sparseKeys setup for cut-off point, fraction and other parameters relevant to determining sparse probability.
   * @return  true if the faceting should be expected to be sparse.
   */
  public boolean isProbablySparse(int hitCount, SparseKeys sparseKeys) {
    if (!initialized) {
      throw new IllegalStateException(
          "It is impossible to calculate sparse probabilities before setFieldProperties has been called");
    }
    if (hitCount == 0 || maxDoc == 0 || referenceCount == 0) {
      return true; // Or false. It makes no difference when the result is known to become empty
    }
    // TODO: Upgrade it to real probability by outcome counting with special casing of single-valued fields
    final double expectedTerms = 1.0 * hitCount / maxDoc * referenceCount;
    final double trackedTerms = sparseKeys.fraction * uniqueValues;
    return uniqueValues >= sparseKeys.minTags && expectedTerms < trackedTerms * sparseKeys.cutOff;
  }

  public String getNotSparseReason(int hitCount, SparseKeys sparseKeys) {
    if (isProbablySparse(hitCount, sparseKeys)) {
      return "logic error: is probably sparse";
    }
    return "hits=" + hitCount + "/" + maxDoc + ", total refs=" + referenceCount + ", unique terms=" + uniqueValues +
        ", fraction=" + sparseKeys.fraction;
  }


  /**
   * @return the maximum amount of counters in this pool.
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public long getReferenceCount() {
    return referenceCount;
  }

  public void setExternalTerms(BytesRefArray externalTerms) {
    this.externalTerms = externalTerms;
  }

  public BytesRefArray getExternalTerms() {
    return externalTerms;
  }

  public long getMaxDoc() {
    return maxDoc;
  }

  public boolean isInitialized() {
    return initialized;
  }

  /**
   * Changing the max triggers a rough cleanup of the pool content is needed.
   * This is not designed to be called frequently with different values.
   * @param max the maximum amount of counters in this pool.
   */
  public void setMaxPoolSize(int max) {
    if (this.maxPoolSize == max) {
      return;
    }
    synchronized (pool) {
      while (pool.size() > max) {
        pool.removeFirst();
      }
      this.maxPoolSize = max;
    }
  }

  /**
   * 0 cleaning threads means that cleaning will be performed upon {@link #release}.
   * @return the amount of Threads used for cleaning counters in the background.
   */
  public int getCleaningThreads() {
    return supervisor.getCorePoolSize();
  }

  /**
   * 0 cleaning threads means that cleaning will be performed upon {@link #release}.
   * @param threads the amount of threads used for cleaning counters in the background.
   */
  public void setCleaningThreads(int threads) {
    synchronized (supervisor) {
      supervisor.setCorePoolSize(threads);
    }
  }

  /**
   * Clears both the pool and any accumulated statistics.
   */
  public void clear() {
    synchronized (pool) {
      pool.clear();
      template = null;
      // TODO: Find a clean way to remove non-started tasks from the cleaner
      structureKey = "discardResultsFromBackgroundClears";

      lastTermsListRequest = "N/A";
      lastTermLookup = "N/A";

      for (TimeStat ts: timeStats) {
        ts.clear();
      }
      for (NumStat ns: numStats) {
        ns.clear();
      }
    }
  }

  public void incWithinCount() {
    withinCutoffCount.inc();
  }
  public void incExceededCount() {
    exceededCutoffCount.inc();
  }
  public void incCollectTimeRel(long startTimeNS) {
    collections.incRel(startTimeNS);
  }
  public void incTermsListTotalTimeRel(long startTimeNS) {
    termsListTotal.incRel(startTimeNS);
  }
  public void incSimpleFacetTotalTimeRel(long startTimeNS) {
    simpleFacetTotal.incRel(startTimeNS);
  }
  public void incExtractTimeRel(long startTimeNS) {
    extractions.incRel(startTimeNS);
  }
  public void incTermResolveTimeRel(long startTimeNS) {
    resolvings.incRel(startTimeNS);
  }
  public void incTermsListLookupRel(String terms, String lastTerm, int existing, int nonExisting, long startTimeNS) {
    lastTermsListRequest = terms;
    lastTermLookup = lastTerm;
    termsListLookup.incRel(startTimeNS);
    termLookup.incRel(existing, startTimeNS);
    termLookupMissing.inc(nonExisting);
  }

  public long getMaxCountForAny() {
    return maxCountForAny;
  }

  public String listCounters() {
    StringBuilder sb = new StringBuilder(100);
    sb.append(Integer.toString(pool.size())).append("(");
    synchronized (pool) {
      for (ValueCounter counter: pool) {
        sb.append(counter.getContentKey()).append(" ");
      }
      sb.append(")").append(", cleaning=").append(Integer.toString(getCleaningThreads()));
    }
    return sb.toString();
  }

  /**
   * @return timing and count statistics for this pool.
   */
  @Override
  public String toString() {
    final int pendingClears = supervisor.getQueue().size() + supervisor.getActiveCount();
    final int poolSize = pool.size();
//    final int cleanerCoreSize = supervisor.getCorePoolSize();
    return String.format(
        "sparse statistics: field(name=%s, description='%s', uniqTerms=%d, maxDoc=%d, refs=%d, maxCountForAny=%d)," +
            "%s, " + // simpleFacetTotal, fallbacks
            "%s, %s, %s, " + // collect, extract, resolve
            "%s, %s, " + // disables,  withinCutoff
            "exceededCutoff=%d, SCPool(cached=%d/%d, currentBackgroundClears=%d, %s, " + // emptyReuses
            "%s, %s, %s, %s, " + // packedAllocations, intAllocations, dualPlaneAllocations, nplaneAllocations
            "%s, " + // regexpMatches
            "%s, %s, " + // requestClears, backgroundClears
            "cache(hits=%d, misses=%d, %s, %s), " + // filledFrees, emptyFrees
            "terms(%s, last#=%d, %s, %s, last=%s, cached=%s)",  // termsListLookup, termLookup, termLookupMissing
        field, description, uniqueValues, maxDoc, referenceCount, maxCountForAny,
        simpleFacetTotal,
        collections, extractions, resolvings,
        disables, withinCutoffCount,
        exceededCutoffCount.get() - disables.get(), poolSize, maxPoolSize, pendingClears, emptyReuses,
        packedAllocations, intAllocations, dualPlaneAllocations, nplaneAllocations,
        regexpMatches,
        requestClears, backgroundClears,
        cacheHits.get(), cacheMisses.get(), filledFrees, emptyFrees,
        termsListLookup, count(lastTermsListRequest, ','), termLookup, termLookupMissing, lastTermLookup,
        externalTerms == null ? "no" : Integer.toString(externalTerms.size()));
  }

  public int getUniqueValues() {
    return uniqueValues;
  }

  public void setUniqueValues(int uniqueValues) {
    this.uniqueValues = uniqueValues;
  }

  private int count(String str, char c) {
    int count = 0;
    for (int i = 0 ; i < str.length() ; i++) {
      if (str.charAt(i) == c) {
        count++;
      }
    }
    return count;
  }

  public NamedList<Object> getStats() {
    final NamedList<Object> stats = new NamedList<>();

    {
      final NamedList<Object> fieldStats = new NamedList<>();
      fieldStats.add("name", field);
      fieldStats.add("description", description);
      fieldStats.add("uniqueTerms", uniqueValues);
      fieldStats.add("maxDoc", maxDoc);
      fieldStats.add("references", referenceCount);
      fieldStats.add("maxCountForAny", maxCountForAny);
      stats.add("field", fieldStats);
    }
    {
      final NamedList<Object> perf = new NamedList<>();
      for (TimeStat ts: timeStats) {
        ts.debug(perf);
      }
      perf.add("currentBackgroundCleans", supervisor.getQueue().size() + supervisor.getActiveCount());
      stats.add("performance", perf);
    }
    {
      final NamedList<Object> calls = new NamedList<>();
      disables.debug(calls);
      withinCutoffCount.debug(calls);
      exceededCutoffCount.debug(calls);
      stats.add("calls", calls);
    }
    {
      final NamedList<Object> cache = new NamedList<>();
      cache.add("content", pool.size() + "/" + maxPoolSize);
      emptyReuses.debug(cache);
      emptyFrees.debug(cache);
      filledReuses.debug(cache);
      filledFrees.debug(cache);
      stats.add("cache", cache);
    }
    {
      final NamedList<Object> terms = new NamedList<>();
      //"terms(%s, last#=%d, %s, %s, last=%s)",  // termsListLookup, termLookup, termLookupMissing
      termsListLookup.debug(terms);
      terms.add("termsListLookupLastCount", count(lastTermsListRequest, ','));
      termLookup.debug(terms);
      termLookupMissing.debug(terms);
      terms.add("termLookupLast", lastTermLookup);
      terms.add("cached", externalTerms == null ? "None" : Integer.toString(externalTerms.size()));
      stats.add("termLookups", terms);
    }

    final NamedList<Object> statsWrap = new NamedList<>();
    statsWrap.add(field, stats);
    return statsWrap;
  }

  /**
   * Puts a Janitor in the job queue. This is a safe operation:
   * If there is nothing to do, the Janitor will finish very quickly.
   */
  private void triggerJanitor() {
    supervisor.execute(new FutureTask<>(new SparsePoolJanitor()));
  }

  public void setMinEmptyCounters(int minEmptyCounters) {
    this.minEmptyCounters = minEmptyCounters;
  }

  /**
   * Locates the best matching counter and return it. Order of priority is<br/>
   * Filled counter matching the given contentKey (if contentKey is != null)<br/>
   * Empty counter<br/>
   * Counter marked {@link #NEEDS_CLEANING}<br/>
   * Filled counter not matching contentKey.
   * @param sparseKeys description of the counter needed.
   * @return a counter removed from the pool if the pool is {@code !pool.isEmpty()}.
   */
  private ValueCounter getCounter(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation) {
    checkConsistency(sparseKeys, implementation);
    return getCounter(sparseKeys.cacheToken);
  }

  private void checkConsistency(SparseKeys sparseKeys, SparseKeys.COUNTER_IMPL implementation) {
    if (!initialized) {
      throw new IllegalStateException("The pool for field '" + field +
          "' has not been initialized (call setFieldProperties for initialization)");
    }
    if (maxCountForAny <= 0 && sparseKeys.counter != SparseKeys.COUNTER_IMPL.array) {
      // We have an empty facet. To avoid problems with the packed structure, we set the maxCountForAny to 1
      maxCountForAny = 1;
    }
    String structureKey = createStructureKey(sparseKeys, implementation);
    synchronized (pool) {
      // Did the structure change since last acquire (index updated)?
      if (!structureKey.equals(this.structureKey)) {
        pool.clear();
        template = null;
      }
      this.structureKey = structureKey;
    }
  }

  /**
   * Locates the best matching counter and return it. Order of priority is<br/>
   * Filled counter matching the given contentKey (if contentKey is != null)<br/>
   * Empty counter<br/>
   * Counter marked {@link #NEEDS_CLEANING}<br/>
   * Filled counter not matching contentKey.
   * @param contentKey optional contentKey for counter re-use. Can be null.
   * @return a counter removed from the pool if the pool is {@code !pool.isEmpty()}.
   */
  private ValueCounter getCounter(String contentKey) {
    synchronized (pool) {
      ValueCounter candidate = null;
      for (ValueCounter vc: pool) {
        if (contentKey != null && contentKey.equals(vc.getContentKey())) {
          candidate = vc;
          break; // It cannot get better than cached counter
        }
        if (vc.getContentKey() == null) {
          candidate = vc;
          break; // Empty counters are always at the end of the pool
        }
        if (NEEDS_CLEANING.equals(vc.getContentKey())) {
          candidate = vc;
          continue; // Maybe there's a contentKey match elsewhere
        }
        if (candidate == null || !NEEDS_CLEANING.equals(candidate.getContentKey())) {
          candidate = vc; // Fallback is the oldest (last in list) non-matching filled counter
        }
      }
      if (candidate != null) {
        // Got a candidate. Remove it from the pool
        pool.remove(candidate);
      }
      return candidate;
    }
  }

  /**
   * Reduces the pool if it is too large and returns a ValueCounter if one needs to be cleaned.
   * This method blocks the pool but does not perform heavy processing.
   * @return a counter in need of cleaning, removed from the pool.
   */
  private ValueCounter reduceAndReturnPool() {
    synchronized (pool) {
      int activeClearing = activeClears.get();

      while (!pool.isEmpty()) {
        int empty = 0;
        for (ValueCounter vc: pool) {
          if (vc.getContentKey() == null) {
            empty++;
          }
        }

        ValueCounter candidate = null;
        for (ValueCounter vc: pool) {
          if (NEEDS_CLEANING.equals(vc.getContentKey())) {
            candidate = vc;
            break; // Any needs cleaning is the best ValueCounter to clean
          }
          if (candidate == null || vc.getContentKey() != null) { // We want the oldest filled if possible
            candidate = vc;
          }
          if (vc.getContentKey() == null) {
            break; // The rest will also be null, so we break early
          }
        }
        assert candidate != null: "There should always be a candidate as pool is not empty";

        // Pool too large?
        if (activeClearing + pool.size() > maxPoolSize) {
          if (empty >= minEmptyCounters) { // Many empty counters. Try to remove one of those to make room for cached
            candidate = pool.getLast();
          }
          pool.remove(candidate);
          if (candidate.getContentKey() == null) {
            emptyFrees.inc();
          } else {
            filledFrees.inc();
          }
          continue;
        }

        // Pool size okay. Anything needs cleaning?
        if (NEEDS_CLEANING.equals(candidate.getContentKey())) {
          // Marked as dirty so clean it
          pool.remove(candidate);
          return candidate;
        }
        if (candidate.getContentKey() == null ||          // Counter already clean
            pool.size() + activeClearing < maxPoolSize || // Pool not full
            empty >= minEmptyCounters) {                  // Enough empty counters
          return null;
        }
        // Pool is full, counter is dirty, we need more empty
        pool.remove(candidate);
        return candidate;
      }
    }
    return null;
  }

  public long[] getHistogram() {
    return histogram;
  }

  public void setHistogram(long[] histogram) {
    this.histogram = histogram;
  }

  public void setPlusOneHistogram(long[] plusOneHistogram) {
    this.plusOneHistogram = plusOneHistogram;
  }

  public long[] getPlusOneHistogram() {
    return plusOneHistogram;
  }

  /**
   * Cleans up the pool if needed.
   */
  private class SparsePoolJanitor implements Callable<ValueCounter> {

    private SparsePoolJanitor() {
    }

    @Override
    public ValueCounter call() throws Exception {
      final long startTime = System.nanoTime();
      ValueCounter dirty = reduceAndReturnPool();
      // Not synchronized, so heavy lifting is okay
      if (dirty != null) {
        if (dirty.getContentKey() == null) { // Sanity check. This should always be false
          releaseCleared(dirty); // No harm done though. We just put the cleared counter back
        } else { // Sanity check. This should always be true
          activeClears.incrementAndGet();
          try {
            String key = dirty.getContentKey();
            dirty.clear();
            backgroundClears.incRel(startTime);
            releaseCleared(dirty);
          } finally {
            activeClears.decrementAndGet();
          }
        }
      }
      return dirty;
    }
  }

  /**
   * Helper class for tracking calls and time spend on a task.
   */
  public class TimeStat {
    public final String name;
    public final int fractionDigits;
    public final NumberFormat numberFormat;
    private long calls = 0;
    private long ns = 0;
    private long lastNS = -1000000;
    private final double M = 1000000.0;

    private TimeStat(String name) {
      this(name, 0);
    }

    public TimeStat(String name, int fractionDigits) {
      this.name = name;
      this.fractionDigits = fractionDigits;
      numberFormat = DecimalFormat.getInstance(Locale.ENGLISH);
      numberFormat.setMinimumFractionDigits(fractionDigits);
      numberFormat.setMaximumFractionDigits(fractionDigits);
      timeStats.add(this);
    }

    public synchronized void incRel(final long startTimeNS) {
      calls++;
      lastNS = System.nanoTime() - startTimeNS;
      ns += lastNS;
    }
    public synchronized void incRel(final int calls, final long startTimeNS) {
      this.calls += calls;
      final long totNS = System.nanoTime() - startTimeNS;
      if (calls != 0) {
        lastNS = totNS / calls;
      }
      ns += totNS;
    }
    public synchronized void clear() {
      calls = 0;
      ns = 0;
      lastNS = -1000000;
    }

    public synchronized String toString() {
      return name + "(" + stats() + ")";
    }
    public String stats() {
      if (calls == 0) {
        return "calls=0";
      }
      return "calls=" + calls + ", avg=" + avg() + "ms" + ", total=" + numberFormat.format(ns / M) +
          "ms, last=" + numberFormat.format(lastNS / M) + "ms";
    }

    private String avg() {
      return calls == 0 ? "N/A" : numberFormat.format(ns / M / calls);
    }

    public void debug(NamedList<Object> debug) {
      debug.add(name, stats());
    }
  }
  
  private class NumStat {
    public final String name;
    private AtomicLong calls = new AtomicLong(0);

    private NumStat(String name) {
      this.name = name;
      numStats.add(this);
    }
    
    public void inc() {
      calls.incrementAndGet();
    }
    public void inc(long delta) {
      calls.addAndGet(delta);
    }
    public long get() {
      return calls.get();
    }
    
    public void clear() {
      calls.set(0);
    }
    
    public String toString() {
      return name + "=" + calls.get();
    }

    public void debug(NamedList<Object> debug) {
      debug.add(name, calls.get());
    }
  }
}

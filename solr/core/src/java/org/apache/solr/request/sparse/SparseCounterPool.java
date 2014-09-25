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
package org.apache.solr.request.sparse;

import org.apache.lucene.util.packed.PackedInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains a pool of SparseCounters, taking care of allocation, used counter clearing and re-use.
 * </p><p>
 * The pool and/or the content of the pool is bound to the index. When the index is updated and a new facet
 * request is issued, the pool will be freed and a new one will be created.
 * </p><p>
 * Constantly allocating large counter structures taxes the garbage collector of the JVM, so it is faster to
 * clear & re-use structures instead. This is especially true for sparse structures, where the clearing time
 * is proportional to the number of previously updated counters.
 * </p><p>
 * The pool has a fixed maximum limit. If there are no cleaned counters available when requested, a new one
 * will be created. If the pool has reached maximum size and a counter is released, the counter is de-referenced,
 * removing it from the heap. It is advisable to set the pool size to a bit more than the number of concurrent
 * faceted searches, to avoid new allocations.
 * </p><p>
 * Setting the pool size very high has the adverse effect of a constant heap overhead, dictated by the maximum
 * number of concurrent facet requests encountered since last index (re)open.
 * </p><p>
 * The pool consists of a mix of empty (ready for any use) and filled (cached for re-use) counters. If the amount
 * of empty counters gets below the stated threshold, filled counters are cleaned and inserted af empty.
 * </p><p>
 * Default behaviour for the pool is to perform counter clearing in 1 background thread, as this makes it possible
 * to provide a facet result to the caller faster. This can be controlled with {@link #setCleaningThreads(int)}.
 * </p><p>
 * This class is thread safe and with no heavy synchronized parts.
 */
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
  private int maxPoolSize;
  private int minEmptyCounters;
  private final AtomicInteger activeClears = new AtomicInteger(0);
  private String structureKey = null;

  // maxCountForAny is a core property as it dictates both size & correctness of packed value counters
  private AtomicLong maxCountForAny = new AtomicLong(-1);

  // Pool stats
  private AtomicLong emptyReuses = new AtomicLong(0);
  private AtomicLong filledReuses = new AtomicLong(0);
  private AtomicLong allocations = new AtomicLong(0);
  private AtomicLong packedAllocations = new AtomicLong(0);
  private AtomicLong clears = new AtomicLong(0);
  private AtomicLong filledFrees = new AtomicLong(0);
  private AtomicLong emptyFrees = new AtomicLong(0);

  // Counter stats
  AtomicLong sparseCalls = new AtomicLong(0);
  AtomicLong skipCount = new AtomicLong(0);
  String lastSkipReason = "no skips";
  AtomicLong sparseAllocateTime = new AtomicLong(0);
  AtomicLong sparseCollectTime = new AtomicLong(0);
  AtomicLong sparseExtractTime = new AtomicLong(0);
  AtomicLong sparseClearTime = new AtomicLong(0);
  AtomicLong sparseTotalTime = new AtomicLong(0);
  AtomicLong disables = new AtomicLong(0);
  AtomicLong withinCutoffCount = new AtomicLong(0);
  AtomicLong exceededCutoffCount = new AtomicLong(0);

  AtomicInteger lastCounts = new AtomicInteger(0);
  String lastTermsLookup = "N/A";
  AtomicLong termsCountsLookups = new AtomicLong(0);
  AtomicLong termsFallbackLookups = new AtomicLong(0);
  String lastTermLookup = "N/A";
  AtomicLong termCountsLookups = new AtomicLong(0);
  AtomicLong termFallbackLookups = new AtomicLong(0);

  AtomicLong cacheHits = new AtomicLong(0);
  AtomicLong cacheMisses = new AtomicLong(0);
  AtomicLong requestClears = new AtomicLong(0);

  AtomicLong termTotalCountTime = new AtomicLong(0);
  AtomicLong termTotalFallbackTime = new AtomicLong(0);

  protected final ThreadPoolExecutor supervisor;
  protected final String field;
  private static final String NEEDS_CLEANING = "DIRTY";

  public SparseCounterPool(ThreadPoolExecutor janitorSupervisor, String field, int maxPoolSize, int minEmptyCounters) {
    this.field = field;
    supervisor = janitorSupervisor;
    this.maxPoolSize = maxPoolSize;
    this.minEmptyCounters = minEmptyCounters;
    pool =  new LinkedList<>();
  }

  /**
   * Delivers a counter ready for updates.
   * </p><p>
   * Note: This assumes that the maximum count for any counter it Integer.MAX_VALUE (2^31).
   * If the maximum value is know, it is highly recommended to use {@link #acquire(int, long, SparseKeys)} instead
   * as that makes it possible to deliver optimized counters.
   * @param counts     the number of entries in the counter.
   * @param sparseKeys generic setup for the Sparse system.
   * @return a counter ready for updates.
   */
  public ValueCounter acquire(int counts, SparseKeys sparseKeys) {
    return acquire(counts, Integer.MAX_VALUE, sparseKeys);
  }

  /**
   * Delivers a counter ready for updates. The type of counter will be chosen based on counts, maxCountForAny and
   * the general Sparse setup from sparseKeys. This is the recommended way to get sparse counters.
   * @param counts     the number of entries in the counter.
   * @param maxCountForAny the maximum value that any individual counter can reach.
   * @param sparseKeys generic setup for the Sparse system.
   * @return a counter ready for updates.
   */
  public ValueCounter acquire(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    final long allocateTime = System.nanoTime();
    if (maxCountForAny <= 0 && sparseKeys.packed) {
      // We have an empty facet. To avoid problems with the packed structure, we set the maxCountForAny to 1
      maxCountForAny = 1;
//      throw new IllegalStateException("Attempted to request sparse counter with maxCountForAny=" + maxCountForAny);
    }
    lastCounts.set(counts);
    try {
      String structureKey = createStructureKey(counts, maxCountForAny, sparseKeys);
      synchronized (pool) {
        // Did the structure change since last acquire (index updated)?
        if (!structureKey.equals(this.structureKey) && !pool.isEmpty()) {
          pool.clear();
        }
        this.structureKey = structureKey;
      }

      ValueCounter vc = getCounter(sparseKeys.cacheToken);
      if (vc == null) { // Got nothing, so we allocate a new one
        return createCounter(counts, maxCountForAny, sparseKeys);
      }

      if (sparseKeys.cacheToken == null) {
        if (vc.getContentKey() == null) { // Asked for empty, got empty
          emptyReuses.incrementAndGet();
          return vc;
        }
        // Asked for empty, got filled
        requestClears.incrementAndGet();
        vc.clear();
        return vc;
      }

      if (vc.getContentKey() == null) { // Asked for filled, got empty
        emptyReuses.incrementAndGet();
        cacheMisses.incrementAndGet();
        return vc;
      } else if (sparseKeys.cacheToken.equals(vc.getContentKey())) { // Asked for filled, got match
        cacheHits.incrementAndGet();
        return vc;
      }
      // Asked for filled, got wrong filled (might just be a NEEDS_CLEANING)
      requestClears.incrementAndGet();
      cacheMisses.incrementAndGet();
      vc.clear();
      return vc;
    } finally {
      incAllocateTime(System.nanoTime() - allocateTime);
    }
  }

  private String createStructureKey(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    return usePacked(counts, maxCountForAny, sparseKeys) ?
        SparseCounterPacked.createStructureKey(
            counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked) :
        SparseCounterInt.createStructureKey(counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction);
  }

  private boolean usePacked(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    return (sparseKeys.packed && PackedInts.bitsRequired(maxCountForAny) <= sparseKeys.packedLimit) ||
        maxCountForAny > Integer.MAX_VALUE;
  }

  private ValueCounter createCounter(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    allocations.incrementAndGet();
    if (maxCountForAny >= 0 && maxCountForAny < Integer.MAX_VALUE) {
      if (this.maxCountForAny.get() != -1) {
        log.warn("Overwrite of existing maxCountForAny " + this.maxCountForAny.get() + " with " + maxCountForAny);
      }
      this.maxCountForAny.set(maxCountForAny);
    }
    if (usePacked(counts, maxCountForAny, sparseKeys)) {
      packedAllocations.incrementAndGet();
      return new SparseCounterPacked(
          counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
    }
    return new SparseCounterInt(
        counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
  }


  /**
   * Release a counter after use. This method will return immediately.
   * </p><p>
   * @param counter a used counter.
   * @param sparseKeys the facet keys associated with the counter.
   */
  public void release(ValueCounter counter, SparseKeys sparseKeys) {
    if (counter.explicitlyDisabled()) {
      disables.incrementAndGet();
    }
    if (structureKey != null && !counter.getStructureKey().equals(structureKey)) {
      // Setup changed, cannot use the counter anymore
      filledFrees.incrementAndGet();
      return;
    }
    if (counter.getContentKey() != null) {
      // If the ValueCounter already has a key, it means that the call must be distributed phase 2 and that the values are not needed anymore
      counter.setContentKey(NEEDS_CLEANING);
    } else {
      // Assign a contentKey to mark the counter as filled. Use the cacheToken if present, for fuure phase 2 re-use
      counter.setContentKey(sparseKeys.cacheToken == null ? NEEDS_CLEANING : sparseKeys.cacheToken);
    }
    synchronized (pool) {
      pool.add(counter);
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
        emptyFrees.incrementAndGet();
        return;
      }
      pool.add(counter);
      structureKey = counter.getStructureKey();
    }
  }

  /**
   * @return the maximum amount of counters in this pool.
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
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
      // TODO: Find a clean way to remove non-started tasks from the cleaner
      structureKey = "discardResultsFromBackgroundClears";
      emptyReuses.set(0);
      filledReuses.set(0);
      allocations.set(0);
      packedAllocations.set(0);
      // No reset of maxCountFor Any as it is potentially heavy to calculate
//      maxCountForAny.set(-1);
      clears.set(0);
      filledFrees.set(0);
      emptyFrees.set(0);

      sparseCalls.set(0);
      skipCount.set(0);
      sparseAllocateTime.set(0);
      sparseCollectTime.set(0);
      sparseExtractTime.set(0);
      sparseClearTime.set(0);
      sparseTotalTime.set(0);
      withinCutoffCount.set(0);
      disables.set(0);
      exceededCutoffCount.set(0);
      lastSkipReason = "no skips";

      lastCounts.set(0);
      lastTermsLookup = "N/A";
      termsCountsLookups.set(0);
      termsFallbackLookups.set(0);
      lastTermLookup = "N/A";
      termCountsLookups.set(0);
      termFallbackLookups.set(0);

      cacheHits.set(0);
      cacheMisses.set(0);
      requestClears.set(0);

      termTotalCountTime.set(0);
      termTotalFallbackTime.set(0);
    }
  }

  public void incSparseCalls() {
    sparseCalls.incrementAndGet();
  }
  public void incSkipCount(String reason) {
    skipCount.incrementAndGet();
    lastSkipReason = reason;
  }
  public void incWithinCount() {
    withinCutoffCount.incrementAndGet();
  }
  public void incExceededCount() {
    exceededCutoffCount.incrementAndGet();
  }
  private void incAllocateTime(long delta) {
    sparseAllocateTime.addAndGet(delta);
  }
  // Nanoseconds
  public void incCollectTime(long delta) {
    sparseCollectTime.addAndGet(delta);
  }
  // Nanoseconds
  public void incExtractTime(long delta) {
    sparseExtractTime.addAndGet(delta);
  }
  private void incClearTime(long delta) {
    sparseClearTime.addAndGet(delta);
  }
  // Nanoseconds
  public void incTotalTime(long delta) {
    sparseTotalTime.addAndGet(delta);
  }
  public void incTermsLookup(String terms, boolean countsStructure) {
    lastTermsLookup = terms;
    if (countsStructure) {
      termsCountsLookups.incrementAndGet();
    } else {
      termsFallbackLookups.incrementAndGet();
    }
  }

  public long getMaxCountForAny() {
    return maxCountForAny.get();
  }

  // Nanoseconds
  public void incTermLookup(String term, boolean countsStructure, long time) {
    lastTermLookup = term;
    if (countsStructure) {
      termCountsLookups.incrementAndGet();
      termTotalCountTime.addAndGet(time);
    } else {
      termFallbackLookups.incrementAndGet();
      termTotalFallbackTime.addAndGet(time);
    }
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
    if (sparseCalls.get() == 0) {
      return "No sparse faceting performed yet";
    }
    final int pendingCleans = supervisor.getQueue().size() + supervisor.getActiveCount();
    final int poolSize = pool.size();
    final int cleanerCoreSize = supervisor.getCorePoolSize();
    return String.format(
        "sparse statistics: uniqTerms=%d, calls=%d, fallbacks=%d (last: %s), " +
            "collect=%dms avg, extract=%dms avg, " +
            "total=%dms avg, disables=%d,  withinCutoff=%d, " +
            "exceededCutoff=%d, SCPool(cached=%d/%d, emptyReuses=%d, " +
            "allocations=%d (%dms avg, %d packed), " +

            "requestClears=%d, " +
            "backgroundClears=%d (%dms avg, %s%s), " +
            "cache(hits=%d, misses=%d)" +
            "filledFrees=%d, emptyFrees=%d, maxCountForAny=%d), terms(count=%d, fallback=%d, " +
            "last#=%d), " +
            "term(count=%d (%.1fms avg), fallback=%d (%.1fms avg), last=%s)",
        lastCounts.get(), sparseCalls.get(), skipCount.get(), lastSkipReason,
        divMint(sparseCollectTime.get(), sparseCalls.get()), divMint(sparseExtractTime.get(), sparseCalls.get()),
        divMint(sparseTotalTime.get(), sparseCalls.get()), disables.get(), withinCutoffCount.get(),
        exceededCutoffCount.get(), poolSize, maxPoolSize, emptyReuses.get(),
        allocations.get(), divMint(sparseAllocateTime.get(), sparseCalls.get()), packedAllocations.get(),

        requestClears.get(),
        clears.get(), divMint(sparseClearTime.get(), sparseCalls.get()), cleanerCoreSize > 0 ? "background" : "at release",
        pendingCleans > 0 ? (" (" + pendingCleans + " running)") : "",
        cacheHits.get(), cacheMisses.get(),
        filledFrees.get(), emptyFrees.get(), maxCountForAny.get(), termsCountsLookups.get(), termsFallbackLookups.get(),
        lastTermsLookup.split(",").length,
        termCountsLookups.get(), divMdouble(termTotalCountTime.get(), termCountsLookups.get()),
        termFallbackLookups.get(), divMdouble(termTotalFallbackTime.get(), termFallbackLookups.get()), lastTermLookup);
  }
  final static int M = 1000000;
  private long divMint(long numerator, long denominator) {
    return denominator == 0 ? 0 : numerator / denominator / M;
  }
  private double divMdouble(long numerator, long denominator) {
    return denominator == 0 ? 0 : numerator * 1.0 / denominator / M;
  }

  /**
   * Puts a Janitor in the job queue. This is a safe operation:
   * If there is nothing to do, the Janitor will finish very quickly.
   */
  private void triggerJanitor() {
    supervisor.execute(new FutureTask<ValueCounter>(new SparsePoolJanitor()));
  }

  public void setMinEmptyCounters(int minEmptyCounters) {
    this.minEmptyCounters = minEmptyCounters;
  }

        // Find the best candidate for re-use. Priority chain is:
        // - Matching cache-key
        // - Empty counter
        // - NEEDS_CLEANING
        // - Filled counter, with non-matching cache-key


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
            emptyFrees.incrementAndGet();
          } else {
            filledFrees.incrementAndGet();
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
      // Not synchronized, so do heavy lifting is okay
      if (dirty != null) {
        if (dirty.getContentKey() == null) { // Sanity check. This should always be false
          releaseCleared(dirty); // No harm done though. We just put the cleared counter back
        } else { // Sanity check. This should always be true
          activeClears.incrementAndGet();
          try {
            dirty.clear();
            clears.incrementAndGet();
            incClearTime(System.nanoTime() - startTime);
            releaseCleared(dirty);
          } finally {
            activeClears.decrementAndGet();
          }
        }
      }
      return dirty;
    }
  }
}

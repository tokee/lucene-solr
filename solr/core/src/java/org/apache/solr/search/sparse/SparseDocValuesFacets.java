package org.apache.solr.search.sparse;

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

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.packed.NPlaneMutable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.DocValuesFacets;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.sparse.cache.SparseCounterPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes term facets for docvalues field (single or multivalued).
 * <p>
 * This is basically a specialized case of the code in SimpleFacets.
 * Instead of working on a top-level reader view (binary-search per docid),
 * it collects per-segment, but maps ordinals to global ordinal space using
 * MultiDocValues' OrdinalMap.
 * <p>
 * This means the ordinal map is created per-reopen: O(nterms), but this may
 * perform better than PerSegmentSingleValuedFaceting which has to merge O(nterms)
 * per query. Additionally it works for multi-valued fields.
 */
public class SparseDocValuesFacets {
  private static Logger log = LoggerFactory.getLogger(SparseDocValuesFacets.class);
  private static final long M = 1000000;

  // TODO: Make the number of threads adjustable
  // TODO: Promote this to a general executor for heavy lifting
  public static final ExecutorService executor = Executors.newFixedThreadPool(20, new ThreadFactory() {
    AtomicLong constructed = new AtomicLong();
    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName("SparseFaceting_" + constructed.getAndIncrement());
      return thread;
    }
  });

  private SparseDocValuesFacets() {} // Static calls only

  // This is a equivalent to {@link UnInvertedField#getCounts} with the extension that it also handles
  // termLists
  public static NamedList<Integer> getCounts(
      SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int minCount, boolean missing,
      String sort, String prefix, String termList, SparseKeys sparseKeys, SparseCounterPool pool) throws IOException {
    if (sparseKeys.logExtended) {
      log.info(sparseKeys.toString() + " q=" + sparseKeys.q);
    }
    if (!sparseKeys.sparse) { // Skip sparse part completely
      return termList == null ?
          DocValuesFacets.getCounts(searcher, docs, fieldName, offset, limit, minCount, missing, sort, prefix) :
          SimpleFacets.fallbackGetListedTermCounts(searcher, null, fieldName, termList, docs);
    }

    final long fullStartTime = System.nanoTime();
    final SparseState state = new SparseState(searcher, docs, fieldName, offset, limit, minCount, missing, sort, prefix,
        termList, sparseKeys, pool);

    if (state.lookup.si == null) {
      return finalize(new NamedList<Integer>(), searcher, state.schemaField, docs, -1, missing);
    }

    // Checks whether we can logically skip faceting

    state.hitCountTime = -System.nanoTime();
    state.hitCount = docs.size();
    state.hitCountTime += System.nanoTime();
    if (!state.keys.useSparse(state.hitCount, state.maxDoc)) {
      if (log.isInfoEnabled()) {
        log.info(String.format(Locale.ENGLISH,
            "Phase 0 sparse faceting of field=%s, hits=%d, maxDoc=%d, boundary=%s skipped as the result set" +
                " size exceeded the boundary",
            fieldName, state.hitCount, state.maxDoc, state.keys.boundary));
      }
      return termList == null ?
          DocValuesFacets.getCounts(searcher, docs, fieldName, offset, limit, minCount, missing, sort, prefix) :
          SimpleFacets.fallbackGetListedTermCounts(searcher, null, fieldName, termList, docs);
    }
    adjustTermIndexes(state);

    if (state.nTerms() <= 0 || state.hitCount < minCount) {
      // Should we count this in statistics? It should be fast and is fairly independent of sparse
      return finalize(new NamedList<Integer>(), searcher, state.schemaField, docs, -1, missing);
    }

    state.heuristic = sparseKeys.useOverallHeuristic(state.hitCount, searcher.maxDoc());
    if (state.heuristic && state.termList != null) { // No reliable counters, switch to slow term counting
      return SimpleFacets.fallbackGetListedTermCounts(state.searcher, null, state.field, state.termList, state.docs);
    }

    // Providers ready. Check that the pool has enough information and construct a counter
    state.acquireTime = -System.nanoTime();
    state.counts = acquireCounter(state);
    state.acquireTime += System.nanoTime();

    // Calculate counts for all relevant terms if the counter structure is empty
    // The counter structure is always empty for single-shard searches and first phase of multi-shard searches
    // Depending on pool cache setup, it might already be full and directly usable in second phase of
    // multi-shard searches
    final boolean alreadyFilled = state.counts.getContentKey() != null;
    state.collectTime = alreadyFilled ? 0 : -System.nanoTime();
    if (!alreadyFilled && !(state.termList != null && state.keys.useFallbackFinecount(state.hitCount, state.maxDoc))) {
      if (!pool.isProbablySparse(state.hitCount, sparseKeys)) {
        // It is guessed that the result set will be to large to be sparse so
        // the sparse tracker is disabled up front to speed up the collection phase
        state.counts.disableSparseTracking();
      }
      org.apache.solr.request.sparse.SparseCount.collectCounts(state);
      state.pool.incCollectTimeRel(state.collectTime);
      state.collectTime += System.nanoTime();
    }

    if (state.termList != null) {
      // Specific terms were requested. This is used with fine-counting of facet values in distributed faceting
      long fineCountStart = System.nanoTime();
      final boolean fallback = state.keys.useFallbackFinecount(state.hitCount, state.maxDoc);
      try {
        return fallback ?
            SimpleFacets.fallbackGetListedTermCounts(searcher, null, fieldName, termList, docs) :
            SparseExtract.extractSpecificCounts(state, state.lookup.si);
      } finally  {
        if (state.effectiveHeuristic || fallback) { // Counts are unreliable so don't store content
          state.counts.setContentKey(SparseCounterPool.NEEDS_CLEANING);
        }
        pool.release(state.counts, sparseKeys);
        pool.incTermsListTotalTimeRel(fullStartTime);
        final long totalTimeNS = Math.max(1, System.nanoTime()-fullStartTime);
        // Maybe this should be on debug instead
        if (log.isInfoEnabled()) {
          log.info(String.format(Locale.ENGLISH,
              "Phase 2 sparse term counts of %s: method=%s, threads=%d, hits=%d, refs=%d, time=%dms "
                  + "(hitCount=%dms, acquire=%dms, collect=%s, fineCount=%dms), hits/ms=%d, refs/ms=%d, " +
                  "heuristic(requested=%b, effective=%b) fallback=%b",
              fieldName, sparseKeys.counter, sparseKeys.countingThreads, state.hitCount, state.refCount.get(),
              totalTimeNS/ M,
              state.hitCountTime/ M, state.acquireTime/ M, alreadyFilled ? "0ms (reused)" : state.collectTime/ M + "ms",
              (System.nanoTime()-fineCountStart)/ M, state.hitCount* M/totalTimeNS, state.refCount.get()* M/totalTimeNS,
              state.heuristic, state.effectiveHeuristic, fallback));
        }
      }
    }

    //final int missingCount = startTermIndex == -1 ? (int) counts.get(0) : -1;

    SparseExtract.extractTopTerms(state);

    if (state.effectiveHeuristic) { // Counts are unreliable for phase 2. Consider  && sparseKeys.heuristicFineCount
      state.counts.setContentKey(SparseCounterPool.NEEDS_CLEANING);
    }
    pool.release(state.counts, sparseKeys);

    pool.incSimpleFacetTotalTimeRel(fullStartTime);
    // Maybe this should be on debug instead
    if (log.isInfoEnabled()) {
      final long totalTimeNS = Math.max(1, System.nanoTime()-fullStartTime);
      log.info(String.format(Locale.ENGLISH,
          "Phase 1 sparse faceting of %s: method=%s, threads=%d, hits=%d, refs=%d, time=%dms "
              + "(hitCount=%dms, acquire=%dms, collect=%s, extract=%dms (sparse=%b), resolve=%dms), hits/ms=%d, " +
              "refs/ms=%d, heuristic(requested=%b, effective=%b)",
          fieldName, sparseKeys.counter, sparseKeys.countingThreads, state.hitCount, state.refCount.get(),
          totalTimeNS/ M,
          state.hitCountTime/ M, state.acquireTime/ M, alreadyFilled ? "0ms (reused)" : state.collectTime/ M + "ms",
          state.extractTime/ M, state.optimizedExtract, state.termResolveTime/ M, state.hitCount* M/totalTimeNS,
          state.refCount.get()* M/totalTimeNS, state.heuristic, state.effectiveHeuristic));
    }
    final int missingCount = state.startTermIndex == -1 ? (int) state.counts.getMissing() : -1;
    return finalize(state.res, searcher, state.schemaField, docs, missingCount, missing);
  }

  private static void adjustTermIndexes(SparseState state) {
    // Locate start and end-position in the ordinals if a prefix is given
    // TODO: Test this for dualplane & nplane counters
    {
      final BytesRef prefixRef;
      if (state.prefix == null) {
        prefixRef = null;
      } else {
        prefixRef = new BytesRef(state.prefix);
      }

      if (state.prefix!=null) {
        state.startTermIndex = (int) state.lookup.si.lookupTerm(prefixRef);
        if (state.startTermIndex<0) state.startTermIndex=-state.startTermIndex-1;
        prefixRef.append(UnicodeUtil.BIG_TERM);
        state.endTermIndex = (int) state.lookup.si.lookupTerm(prefixRef);
        assert state.endTermIndex < 0;
        state.endTermIndex = -state.endTermIndex-1;
      } else {
        //startTermIndex=-1;
        state.startTermIndex=0; // Changed to explicit missingCount
        state.endTermIndex=(int) state.lookup.si.getValueCount();
      }
    }
  }

  /*
   * Determines the maxOrdCount for any term in the given field by looping through all live docIDs and summing the
   * termOrds. This is needed for proper setup of {@link SparseCounterPacked}.
   * Note: This temporarily allocates an int[maxDoc]. Fortunately this happens before standard counter allocation
   * so this should not blow the heap.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static ValueCounter acquireCounter(SparseState state) throws IOException {
    // Overall the code gets a bit muddy as we do lazy extraction of meta data about the ordinals
    // state.lookup.si, state.lookup.ordinalMap
    final SparseKeys sparseKeys = state.keys;
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.array) {
      ensureBasic(state);
      return state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.array);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.packed) {
      ensureBasic(state);
      return state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.packed);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.auto) {
      ensureBasic(state);
      return state.pool.usePacked(sparseKeys) ?
          state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.packed) :
          state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.array);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.dualplane) {
      ensureBasic(state);
      return state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.dualplane);
    }
    // nplane is very heavy at first call: Its overflow bits needs the maximum counts for all ordinals
    // to be calculated.
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.nplane) {
      ValueCounter vc;
      synchronized (state.pool) { // Need to synchronize to avoid overlapping BPV-resolving
        if (!state.pool.isInitialized() ||
            ((vc = state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.nplane)) == null)) {
          final long allocateTime = System.nanoTime();
          NPlaneMutable.BPVProvider bpvs = ensureBasicAndGetBPVs(state);
          NPlaneMutable.Layout layout = NPlaneMutable.getLayout(state.pool.getHistogram(), false);
          // TODO: Consider switching back and forth between threaded and non-threaded
          PackedInts.Mutable innerCounter = new NPlaneMutable(layout, bpvs, NPlaneMutable.IMPL.tank);
          vc = new SparseCounterThreaded(SparseKeys.COUNTER_IMPL.nplane, innerCounter, state.pool.getMaxCountForAny(),
              sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
          state.pool.addAndReturn(sparseKeys, SparseKeys.COUNTER_IMPL.nplane, vc, allocateTime);
        }
        return vc;
      }
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.nplanez) {
      ValueCounter vc;
      // TODO: Check that acquiring a nplanex after a nplane erases the old nplane
      synchronized (state.pool) { // Need to synchronize to avoid overlapping BPV-resolving
        if (!state.pool.isInitialized() ||
            ((vc = state.pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.nplanez)) == null)) {
          final long allocateTime = System.nanoTime();
          NPlaneMutable.BPVProvider bpvs = ensureBasicAndGetBPVs(state);
          NPlaneMutable.Layout layout = NPlaneMutable.getLayout(state.pool.getPlusOneHistogram(), true);
          NPlaneMutable innerCounter = new NPlaneMutable(layout, bpvs, NPlaneMutable.IMPL.zethra);
//        System.out.println(innerCounter.toString(true));
          vc = new SparseCounterBitmap(SparseKeys.COUNTER_IMPL.nplanez, innerCounter, state.pool.getMaxCountForAny(),
              sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
          state.pool.addAndReturn(sparseKeys, SparseKeys.COUNTER_IMPL.nplanez, vc, allocateTime);
        }
        return vc;
      }
    }
    throw new UnsupportedOperationException("No support yet for the " + sparseKeys.counter + " counter");
  }

  // Not synchronized as that must be handled outside to avoid duplicate work
  private static NPlaneMutable.BPVProvider ensureBasicAndGetBPVs(SparseState state) throws IOException {
    NPlaneMutable.BPVProvider globOrdCount = OrdinalUtils.getBPVs(
        state.searcher, state.lookup.si, state.lookup.ordinalMap, state.schemaField, true);
    // It would be nice to skip this extra run-through, but nplane needs its histogram
    ensureBasic(globOrdCount, state.searcher, state.lookup.si, state.schemaField, state.pool);
    globOrdCount.reset();
    return globOrdCount;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We update the pool we synchronize on
  private static void ensureBasic(SparseState state) throws IOException {
    synchronized (state.pool) {
      if (state.pool.isInitialized()) {
        return;
      }
      ensureBasic(
          OrdinalUtils.getBPVs(state.searcher, state.lookup.si, state.lookup.ordinalMap, state.schemaField, true),
          state.searcher, state.lookup.si, state.schemaField, state.pool);
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We update the pool we synchronize on
  private static void ensureBasic(NPlaneMutable.BPVProvider globOrdCount, SolrIndexSearcher searcher,
                                  SortedSetDocValues si, SchemaField schemaField, SparseCounterPool pool) {
    synchronized (pool) {
      if (pool.isInitialized()) {
        return;
      }
    }
    final long startTime = System.nanoTime();
    NPlaneMutable.StatCollectingBPVWrapper stats = globOrdCount instanceof NPlaneMutable.StatCollectingBPVWrapper ?
        (NPlaneMutable.StatCollectingBPVWrapper) globOrdCount : new NPlaneMutable.StatCollectingBPVWrapper(globOrdCount);
    stats.collect();
//    System.out.println(stats);
    log.info(String.format(Locale.ENGLISH,
        "Calculated maxCountForAny=%d for field %s with %d references to %d unique values in %dms. Histogram: %s",
        stats.maxCount, schemaField.getName(), stats.refCount, stats.entries,
        (System.nanoTime() - startTime) / 1000000, OrdinalUtils.join(stats.histogram)));
    pool.setFieldProperties((int) (si.getValueCount() + 1), stats.maxCount, searcher.maxDoc(), stats.refCount);
    pool.setHistogram(stats.histogram);
    pool.setPlusOneHistogram(stats.plusOneHistogram);

/*    System.out.print("********** " + schemaField.getName() + "\nRaw values: ");
    stats.reset();
    while (stats.hasNext()) {
      System.out.print(stats.nextValue() + " ");
    }
    System.out.print("\nHistogram+: ");
    for (long l: stats.plusOneHistogram) {
      System.out.print(l + " ");
    }
    System.out.println();*/
  }


  /**************** Sparse implementation end *******************/

  /** finalizes result: computes missing count if applicable */
  static NamedList<Integer> finalize(NamedList<Integer> res, SolrIndexSearcher searcher, SchemaField schemaField,
                                     DocSet docs, int missingCount, boolean missing) throws IOException {
    if (missing) {
      if (missingCount < 0) {
        missingCount = SimpleFacets.getFieldMissingCount(searcher,docs,schemaField.getName());
      }
      res.add(null, missingCount);
    }

    return res;
  }

}

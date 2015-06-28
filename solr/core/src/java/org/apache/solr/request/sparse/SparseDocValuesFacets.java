package org.apache.solr.request.sparse;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.packed.NPlaneMutable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.DocValuesFacets;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.LongPriorityQueue;
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
  public static Logger log = LoggerFactory.getLogger(SparseDocValuesFacets.class);

  // TODO: Make the number of threads adjustable
  // TODO: Promote this to a general executor for heavy lifting
  static final ExecutorService executor = Executors.newFixedThreadPool(20, new ThreadFactory() {
    AtomicLong constructed = new AtomicLong();
    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName("SparseFaceting_" + constructed.getAndIncrement());
      return thread;
    }
  });

  private SparseDocValuesFacets() {}

  private static class SparseState {
    public final SolrIndexSearcher searcher;
    public final int maxDoc;
    public final DocSet docs;
    public final String field;
    public final SchemaField schemaField;
    public final int offset;
    public final int limit;
    public final int minCount;
    public final boolean missing;
    public final String sort;
    public final String prefix;
    public final String termList;
    public final SparseKeys keys;
    public final SparseCounterPool pool;
    public final Lookup lookup;

    public final AtomicLong refCount = new AtomicLong(0); // Number of references from docIDs to ordinals (#increments)
    public int hitCount;
    public int startTermIndex;
    public int endTermIndex;
    public boolean heuristic;
    public ValueCounter counts;
    public final NamedList<Integer> res = new NamedList<>();
    public boolean optimizedExtract;

    public long hitCountTime;
    public long acquireTime;
    public long collectTime;
    public long extractTime;
    public long termResolveTime;

    private SparseState(
        SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int minCount,
        boolean missing, String sort, String prefix, String termList, SparseKeys keys, SparseCounterPool pool)
        throws IOException {
      this.searcher = searcher;
      this.maxDoc = searcher.maxDoc();
      this.docs = docs;
      this.field = field;
      this.offset = offset;
      this.limit = limit;
      this.minCount = minCount;
      this.missing = missing;
      this.sort = sort;
      this.prefix = "".equals(prefix) ? null : prefix;
      this.termList = termList;
      this.keys = keys;
      this.pool = pool;
      schemaField = searcher.getSchema().getField(field);
      lookup = new Lookup(searcher, schemaField);
    }

    public int nTerms() {
      return endTermIndex-startTermIndex;
    }
  }

  // This is a equivalent to {@link UnInvertedField#getCounts} with the extension that it also handles
  // termLists
  public static NamedList<Integer> getCounts(
      SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int minCount, boolean missing,
      String sort, String prefix, String termList, SparseKeys sparseKeys, SparseCounterPool pool) throws IOException {
    if (sparseKeys.logExtended) {
      log.info(sparseKeys.toString());
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
    if (!alreadyFilled) {
      if (!pool.isProbablySparse(state.hitCount, sparseKeys)) {
        // It is guessed that the result set will be to large to be sparse so
        // the sparse tracker is disabled up front to speed up the collection phase
        state.counts.disableSparseTracking();
      }
      collectCounts(state);
      state.pool.incCollectTimeRel(state.collectTime);
      state.collectTime += System.nanoTime();
    }

    if (state.termList != null) {
      // Specific terms were requested. This is used with fine-counting of facet values in distributed faceting
      long fineCountStart = System.nanoTime();
      try {
        return extractSpecificCounts(state, state.lookup.si);
      } finally  {
        if (state.heuristic && sparseKeys.heuristicFineCount) { // Counts are unreliable so don't store
          state.counts.setContentKey(SparseCounterPool.NEEDS_CLEANING);
        }
        pool.release(state.counts, sparseKeys);
        pool.incTermsListTotalTimeRel(fullStartTime);
        final long totalTimeNS = Math.max(1, System.nanoTime()-fullStartTime);
        // Maybe this should be on debug instead
        if (log.isInfoEnabled()) {
          log.info(String.format(Locale.ENGLISH,
              "Phase 2 sparse term counts of %s: method=%s, threads=%d, hits=%d, refs=%d, time=%dms "
                  + "(hitCount=%dms, acquire=%dms, collect=%s, fineCount=%dms), hits/ms=%d, refs/ms=%d, heuristic=%b",
              fieldName, sparseKeys.counter, sparseKeys.countingThreads, state.hitCount, state.refCount.get(),
              totalTimeNS/M,
              state.hitCountTime/M, state.acquireTime/M, alreadyFilled ? "0ms (reused)" : state.collectTime/M + "ms",
              (System.nanoTime()-fineCountStart)/M, state.hitCount*M/totalTimeNS, state.refCount.get()*M/totalTimeNS,
              state.heuristic));
        }
      }
    }

    //final int missingCount = startTermIndex == -1 ? (int) counts.get(0) : -1;

    extractTopTerms(state);

    if (state.heuristic && sparseKeys.heuristicFineCount) { // Counts are unreliable for phase 2
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
              "refs/ms=%d, heuristic=%b",
          fieldName, sparseKeys.counter, sparseKeys.countingThreads, state.hitCount, state.refCount.get(),
          totalTimeNS/M,
          state.hitCountTime/M, state.acquireTime/M, alreadyFilled ? "0ms (reused)" : state.collectTime/M + "ms",
          state.extractTime/M, state.optimizedExtract, state.termResolveTime/M, state.hitCount*M/totalTimeNS,
          state.refCount.get()*M/totalTimeNS, state.heuristic));
    }
    final int missingCount = state.startTermIndex == -1 ? (int) state.counts.getMissing() : -1;
    return finalize(state.res, searcher, state.schemaField, docs, missingCount, missing);
  }

  private static void extractTopTerms(SparseState state) throws IOException {
    if (state.sort.equals(FacetParams.FACET_SORT_COUNT) || state.sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
      extractTopTermsCount(state);
    } else {
      extractTopTermsIndex(state);
    }
  }

  private static void extractTopTermsIndex(SparseState state) {
    int off=state.offset;
    int lim=state.limit>=0 ? state.limit : Integer.MAX_VALUE;
    final FieldType ft = state.schemaField.getType();
    final CharsRef charsRef = new CharsRef(10);
    state.extractTime = System.nanoTime();
    state.optimizedExtract = false;
    state.termResolveTime = System.nanoTime();
    // add results in index order
    int i=(state.startTermIndex==-1)?1:0;
    if (state.minCount<=0) {
      // if mincount<=0, then we won't discard any terms and we know exactly
      // where to start.
      i+=off;
      off=0;
    }
    // TODO: Add black- & white-list
    for (; i<state.nTerms(); i++) {
      int c = (int) state.counts.get(i);
      if (c<state.minCount || --off>=0) continue;
      if (--lim<0) break;
      state.res.add(resolveTerm(state.pool, state.keys, state.lookup.si, ft, state.startTermIndex+i, charsRef), c);
/*        si.lookupOrd(startTermIndex+i, br);
      ft.indexedToReadable(br, charsRef);
      res.add(charsRef.toString(), c);*/
    }
    state.pool.incTermResolveTimeRel(state.termResolveTime);
    state.termResolveTime = System.nanoTime()-state.termResolveTime;
    state.extractTime = 0; // Only resolving is relevant here
  }

  private static void extractTopTermsCount(SparseState state) throws IOException {
    int off=state.offset;
    int lim=state.limit>=0 ? state.limit : Integer.MAX_VALUE;
    final FieldType ft = state.schemaField.getType();
    final CharsRef charsRef = new CharsRef(10);
    state.extractTime = System.nanoTime();
    int maxsize = state.limit>0 ? state.offset+state.limit : Integer.MAX_VALUE-1;
    maxsize = Math.min(maxsize, state.nTerms());
    LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

//        int min=mincount-1;  // the smallest value in the top 'N' values

    try {
      state.optimizedExtract = state.counts.iterate(
          state.startTermIndex==-1?1:0, state.nTerms(), state.minCount, false,
          state.keys.blacklists.isEmpty() && state.keys.whitelists.isEmpty() ?
              new ValueCounter.TopCallback(state.minCount-1, queue) :
              new PatternMatchingCallback(
                  state.minCount-1, queue, maxsize, state.keys, state.pool, state.lookup.si, ft, charsRef));
      if (state.optimizedExtract) {
        state.pool.incWithinCount();
      } else {
        state.pool.incExceededCount();
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new ArrayIndexOutOfBoundsException(String.format(
          "Logic error: Out of bounds with startTermIndex=%d, nTerms=%d, minCount=%d and counts=%s",
          state.startTermIndex, state.nTerms(), state.minCount, state.counts));
    }
    state.pool.incExtractTimeRel(state.extractTime);
    state.extractTime = System.nanoTime()-state.extractTime;

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= lim;

    state.termResolveTime = System.nanoTime();
    // the start and end indexes of our list "sorted" (starting with the highest value)
    int sortedIdxStart = queue.size() - (collectCount - 1);
    int sortedIdxEnd = queue.size() + 1;
    final long[] sorted = queue.sort(collectCount);

    for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
      long pair = sorted[i];
      int count = (int)(pair >>> 32);
      int tnum = Integer.MAX_VALUE - (int)pair;
      final String term = resolveTerm(
          state.pool, state.keys, state.lookup.si, ft, state.startTermIndex+tnum, charsRef);
      if (state.heuristic && state.keys.heuristicFineCount) { // Need to fine-count
        // TODO: Add heuristics lookup-stats to pool
        state.res.add(term, state.searcher.numDocs(
            new TermQuery(new Term(state.field, ft.toInternal(term))), state.docs));
      } else {
        state.res.add(term, count);
      }
/*        si.lookupOrd(startTermIndex+tnum, br);
      ft.indexedToReadable(br, charsRef);
      res.add(charsRef.toString(), c);*/
    }
    if (state.heuristic && state.keys.heuristicFineCount) { // Order might be off
      sortByCount(state.res);
    }
    state.pool.incTermResolveTimeRel(state.termResolveTime);
    state.termResolveTime = System.nanoTime()-state.termResolveTime;
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

  private static void sortByCount(NamedList<Integer> res) {
    List<SimpleFacets.CountPair<String, Integer>> entries = new ArrayList<>(res.size());
    for (Map.Entry<String, Integer> next : res) {
      entries.add(new SimpleFacets.CountPair<>(next.getKey(), next.getValue()));
    }
    Collections.sort(entries, new Comparator<SimpleFacets.CountPair<String, Integer>>() {
      @Override
      public int compare(SimpleFacets.CountPair<String, Integer> o1, SimpleFacets.CountPair<String, Integer> o2) {
        int cs = -o1.val.compareTo(o2.val);
        return cs != 0 ? cs : o1.key.compareTo(o2.key);
      }
    });
    res.clear();
    for (SimpleFacets.CountPair<String, Integer> entry: entries) {
      res.add(entry.key, entry.val);
    }
  }

  /**
   * Encapsulation of DocValues and OrdinalMap. Neither are Thread safe so an instance/Thread is needed.
   */
  private static class Lookup {
    private final SchemaField schemaField;
    private final SolrIndexSearcher searcher;

    final SortedSetDocValues si;
    final OrdinalMap ordinalMap;

    public Lookup(SolrIndexSearcher searcher, SchemaField schemaField) throws IOException {
      this.schemaField = schemaField;
      this.searcher = searcher;

      String fieldName = schemaField.getName();
      if (schemaField.multiValued()) {
        si = searcher.getAtomicReader().getSortedSetDocValues(fieldName);
        ordinalMap = si instanceof MultiSortedSetDocValues ? ((MultiSortedSetDocValues)si).mapping : null;
      } else {
        SortedDocValues single = searcher.getAtomicReader().getSortedDocValues(fieldName);
        si = single == null ? null : DocValues.singleton(single);
        ordinalMap = single instanceof MultiSortedDocValues ? ((MultiSortedDocValues)single).mapping : null;
      }
      if (si != null && si.getValueCount() >= Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "Currently this faceting method is limited to Integer.MAX_VALUE=" + Integer.MAX_VALUE + " unique terms");
      }
    }

    /**
     * Call this from another Thread than the one constructing this to get a Thread-local instance.
     * @return a Thread-local instance of Lookup with suitable DocValues and OrdinalMap structures.
     * @throws IOException if the cloning could not be performed.
     */
    public Lookup cloneToThread() throws IOException {
      return new Lookup(searcher, schemaField);
    }

    public int getGlobalOrd(int segmentIndex, int segmentOrdinal) {
      return ordinalMap == null ? segmentOrdinal : (int) ordinalMap.getGlobalOrds(segmentIndex).get(segmentOrdinal);
    }
  }

  /*
  Iterate the leafs and update the counters based on the DocValues ordinals, adjusted to the global ordinal structure.
   */
  private static void collectCounts(SparseState state) throws IOException {
    final Filter filter = state.docs.getTopFilter(); // Should be Thread safe (see PerSegmentSingleValuedFaceting)
    final List<AtomicReaderContext> leaves = state.searcher.getTopReaderContext().leaves();
    // The queue ensures that only the given max of threads are started
    // Accumulators are responsible for removing themselves from the queue
    // after counting has finished or failed
    final BlockingQueue<Accumulator> accumulators = state.keys.countingThreads <= 1 ? null :
        new LinkedBlockingQueue<Accumulator>(state.keys.countingThreads);

    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
      final int leafMaxDoc = leaf.reader().maxDoc();
      if (leafMaxDoc == 0) {
        continue;
      }

      if (accumulators == null) { // In-thread counting
        try {
          new Accumulator(leaf, 0, leafMaxDoc, state, filter, subIndex, null, false).call();
          continue;
        } catch (Exception e) {
          throw new IOException("Exception calling " + Accumulator.class.getSimpleName() + " within currentThread", e);
        }
      }

      // Multi threaded counting
      // Determine the number of parts to split the docIDspace into, taking into account the maximum number
      // of threads as well as the minimum size of a part
      final int parts = state.hitCount < state.keys.countingThreadsMinDocs ? 1 : // Overall hitCount
          Math.max(1, Math.min(state.keys.countingThreads, leafMaxDoc / state.keys.countingThreadsMinDocs)); // Leaf
      final int blockSize = Math.max(1, leafMaxDoc / parts);
      //System.out.println(String.format("maxDoc=%d, parts=%d, blockSize=%d", leaf.reader().maxDoc(), parts, blockSize));
      for (int i = 0 ; i < parts ; i++) {
        // FIXME: The heuristic logic has not been thought through with threading
        Accumulator accumulator = new Accumulator(
            leaf, i*blockSize, i < parts-1 ? (i+1)*blockSize : leafMaxDoc, state, filter, subIndex,
            accumulators, i != 0);
        try {
          accumulators.put(accumulator);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while putting new Accumulator into the queue", e);
        }
        executor.submit(accumulator);
      }
    }
    if (accumulators == null) {
      return;
    }
    emptyQueue(state.keys, accumulators); // Consider doing this at the very end instead
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We synchronize to wait
  private static void emptyQueue(SparseKeys sparseKeys, BlockingQueue<Accumulator> accumulators) throws IOException {
    // Wait for the threads to finish
    try {
      synchronized (accumulators) {
        while (!accumulators.isEmpty()) {
          accumulators.wait();
        }
      }
    } catch (InterruptedException e) {
      throw new IOException("Exception waiting for accumulators to finish with " + sparseKeys, e);
    }
  }

  private static class Accumulator implements Callable<Accumulator> {
    private final AtomicReaderContext leaf;
    private final SparseState state;
    private final int startDocID;
    private final int endDocID;
    private final Filter filter;
    private final int subIndex;
    private final Queue<Accumulator> accumulators;
    private final boolean cloneLookup;
    private final boolean heuristic;
    private Lookup lookup;

    public Accumulator(
        AtomicReaderContext leaf, int startDocID, int endDocID, SparseState state, Filter filter,
        int subIndex, Queue<Accumulator> accumulators, boolean multiThreadedInLeaf) {
      this.state = state;
      this.leaf = leaf;
      this.startDocID = startDocID;
      this.endDocID = endDocID;
      this.filter = filter;
      this.subIndex = subIndex;
      this.accumulators = accumulators;
      this.cloneLookup = multiThreadedInLeaf;
      this.heuristic = state.heuristic &&
          state.keys.useSegmentHeuristics(state.hitCount, state.maxDoc, endDocID-startDocID);
      this.lookup = state.lookup;
    }

    @Override
    public Accumulator call() throws Exception {
      try {
        final long startTime = System.nanoTime();
        if (cloneLookup) { // TODO: Verify that we really need to do this (hard as the culprit is thread collisions)
          lookup = lookup.cloneToThread();
        }
        // TODO: Check if dis.bits() are available and share them instead of re-issuing a lot of searches
        DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
        DocIdSetIterator disi = null;
        if (dis != null) {
          disi = dis.iterator();
        }
        if (disi == null) {
          return this;
        }
        if (state.schemaField.multiValued()) {
          SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(state.field);
          if (sub == null) {
            sub = DocValues.emptySortedSet();
          }
          final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            accumSingle(state, singleton, disi, startDocID, endDocID, subIndex, lookup, System.nanoTime() - startTime,                heuristic);
          } else {
            accumMulti(state, sub, disi, startDocID, endDocID, subIndex, lookup, System.nanoTime() - startTime,
                heuristic);
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(state.field);
          if (sub == null) {
            sub = DocValues.emptySorted();
          }
          accumSingle(state, sub, disi, startDocID, endDocID, subIndex, lookup, System.nanoTime() - startTime, heuristic);
        }
        return this;
      } finally {
        if (accumulators != null) {
          synchronized (accumulators) {
            if (!accumulators.remove(this)) {
              log.warn("Logic error: Self-removal of the current Accumulator did not evict anything from the queue. "
                  + "This indicates a queueing problem and possibly a non-terminating call, tying up resources");
            }
            accumulators.notify();
          }
        }
      }
    }

  }
  /**
   * Resolves an ordinal to an external String. The lookup might be cached.
   *
   * @param pool       the Searcher-instance-specific pool, potentially holding a cache of lookup terms.
   * @param sparseKeys the sparse request, stating whether caching of terms should be used or not.
   * @param si         term provider.
   * @param ft         field type for mapping internal term representation to external.
   * @param ordinal    the ordinal for the requested term.
   * @param charsRef   independent CharsRef to avoid object allocation.
   * @return an external String directly usable for constructing facet response.
   */
  // TODO: Remove brNotUsedAnymore
  private static String resolveTerm(SparseCounterPool pool, SparseKeys sparseKeys, SortedSetDocValues si,
                                    FieldType ft, int ordinal, CharsRef charsRef) {
    // TODO: Check that the cache spans segments instead of just handling one
    if (sparseKeys.termLookupMaxCache > 0 && sparseKeys.termLookupMaxCache >= si.getValueCount()) {
      BytesRefArray brf = pool.getExternalTerms();
      if (brf == null) {
        // Fill term cache
        brf = new BytesRefArray(Counter.newCounter()); // TODO: Incorporate this in the overall counters
        for (int ord = 0; ord < si.getValueCount(); ord++) {
          BytesRef br = si.lookupOrd(ord);
          ft.indexedToReadable(br, charsRef);
          brf.append(new BytesRef(charsRef)); // Is the object allocation avoidable?
        }
        pool.setExternalTerms(brf);
      }
      // TODO: Re-use the BytesRefBuilder
      return brf.get(new BytesRefBuilder(), ordinal).utf8ToString();
    }
    pool.setExternalTerms(null); // Make sure the memory is freed
    // No cache, so lookup directly
    BytesRef br = si.lookupOrd(ordinal);
    ft.indexedToReadable(br, charsRef);
    return charsRef.toString();
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

  private static String join(long[] values) {
    StringBuilder sb = new StringBuilder(values.length*5);
    for (long value: values) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(value));
    }
    return sb.toString();
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
        (System.nanoTime() - startTime) / 1000000, join(stats.histogram)));
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

  static int warnedOrdinal = 0;
  private static NamedList<Integer> extractSpecificCounts(SparseState state, SortedSetDocValues si) throws IOException {
    // TODO: Extend this to use the pool.getExternalTerms if present
    final long startTime = System.nanoTime();
    final List<String> terms = StrUtils.splitSmart(state.termList, ",", true);
    final FieldType ft = state.searcher.getSchema().getFieldType(state.field);

    int existingTerms = 0;
    NamedList<Integer> res = new NamedList<>();
    for (String term : terms) {
      String internal = ft.toInternal(term);
      // Direct lookup as we removed the missing-values-in-counter-zero hack
      long index = si.lookupTerm(new BytesRef(term));
      // TODO: Remove err-out after sufficiently testing
      int count;
      if (index < 0) { // This is OK. Asking for a non-existing term is normal in distributed faceting
        count = 0;
      } else if (index >= state.counts.size()) {
        if (warnedOrdinal != state.searcher.hashCode()) {
          log.warn("DocValuesFacet.extractSpecificCounts: ordinal for " + term + " in field " + state.field + " was "
              + index + " but the counts only go from 0 to ordinal " + state.counts.size() + ". Switching to " +
              "searcher.numDocs. This warning will not be repeated until the index has been updated.");
          warnedOrdinal = state.searcher.hashCode();
        }
        count = state.searcher.numDocs(new TermQuery(new Term(state.field, internal)), state.docs);
      } else if (state.heuristic && state.keys.heuristicFineCount) {
        count = state.searcher.numDocs(new TermQuery(new Term(state.field, internal)), state.docs);
      } else {
        count = (int) state.counts.get((int) index);
        existingTerms++;
      }
      res.add(term, count);
    }
    state.pool.incTermsListLookupRel(state.termList, !terms.isEmpty() ? terms.get(terms.size()-1) : "",
        existingTerms, terms.size()-existingTerms, startTime);
    return res;
  }

  /*
  Iterate single-value DocValues and update the counters based on delivered ordinals.
   */
  private static void accumSingleOrig(
      SparseState state, SortedDocValues si, DocIdSetIterator disi,
      int startDocID, int endDocID, int subIndex, Lookup lookup, long initNS, boolean heuristic) throws IOException {
    final long startTime = System.nanoTime();
    final int segmentSampleSize = state.keys.segmentSampleSize(state.hitCount, state.maxDoc, endDocID - startDocID + 1);
    final int hChunks = !heuristic ? 1 : state.keys.heuristicSampleChunks;
    final int hChunkSize = !heuristic ? endDocID-startDocID+1 : segmentSampleSize / hChunks;
    final int hChunkSkip = (endDocID-startDocID+1)/hChunks;
    int doc = disi.nextDoc();
    if (startDocID > 0 && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;
    long docs = 0;
    long increments = 0;

    for (int chunk = 0 ; chunk < hChunks ; chunk++) { // heuristic
      if (doc < startDocID+chunk*hChunkSkip && doc != DocIdSetIterator.NO_MORE_DOCS) {
        doc = disi.advance(startDocID+chunk*hChunkSkip);
      }
      final int chunkEnd = Math.min(endDocID, startDocID+chunk*hChunkSkip+hChunkSize);
      while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < chunkEnd) {
        docs++;
        int term = si.getOrd(doc);
        if (lookup.ordinalMap != null && term >= 0) {
          term = lookup.getGlobalOrd(subIndex, term);
        }
        int arrIdx = term-state.startTermIndex;
        if (arrIdx>=0 && arrIdx<state.counts.size()) {
          increments++;
          state.counts.inc(arrIdx);
        }
        doc = disi.nextDoc();
      }
    }
    // TODO: Remove this when sparse faceting is considered stable
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      final double sampleFactor =
          state.keys.segmentSampleFactor(state.hitCount, state.maxDoc, endDocID - startDocID + 1);
      log.info(String.format(Locale.ENGLISH,
          "accumSingle(%d->%d) impl=%s, init=%dms, advance=%dms, docHits=%d, increments=%d (%d incs/doc)," +
              " incTime=%dms (%d incs/ms, %d docs/ms), heuristic=%b (chunks=%d, chunkSize=%d, skip=%d, factor=%f)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M, docs, increments,
          docs == 0 ? 0 : increments/docs, incNS / M, incNS == 0 ? 0 : increments * M / incNS,
          incNS == 0 ? 0 : docs * M / incNS, heuristic, hChunks, hChunkSize, hChunkSkip, sampleFactor));
    }
    state.refCount.addAndGet(increments);
  }
  private static void accumSingle(
      final SparseState state, final SortedDocValues si, final DocIdSetIterator disi,
      final int startDocID, final int endDocID, final int subIndex, final Lookup lookup,
      // TODO: Remove initNS
      long initNS, final boolean heuristic) throws IOException {
    final long startTime = System.nanoTime();

    final int maxSampleDocs = state.keys.segmentSampleSize(state.hitCount, state.maxDoc, endDocID - startDocID + 1);
    int maxChunks = !heuristic ? 1 :
        Math.max(
            1,
            Math.min(
                state.hitCount / state.keys.heuristicSampleChunksMinSize,
                state.keys.heuristicSampleChunks
            )
        );

    int doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;
    final double sampleFactor =
        state.keys.segmentSampleFactor(state.hitCount, state.maxDoc, endDocID - startDocID + 1);

    int visitedChunks = 0;
    int sampleDocs = 0;
    long references = 0;

    while (visitedChunks < maxChunks && sampleDocs < maxSampleDocs && doc != DocIdSetIterator.NO_MORE_DOCS) {
      final int missingSamples = maxSampleDocs - sampleDocs;
      final int missingChunks = maxChunks - visitedChunks;

      final int chunkSize = Math.max(state.keys.heuristicSampleChunksMinSize, missingSamples/missingChunks);
      final int nextChunkStart = doc + (endDocID-doc+1)/missingChunks;
      final int chunkDocGoal = sampleDocs + chunkSize;

      log.info(String.format("*** doc=%d, sampleDocs=%d, missingSamples=%d, missingChunks=%d, chunkSize=%d," +
              " chunkDocGoal=%d, nextChunkStart=%d, indexHits=%d, sampleFactor=%.3f",
          doc, sampleDocs, missingSamples, missingChunks, chunkSize,
          chunkDocGoal, nextChunkStart, state.hitCount, sampleFactor));

      while (sampleDocs < chunkDocGoal && doc != DocIdSetIterator.NO_MORE_DOCS) {
        sampleDocs++;
        int term = si.getOrd(doc);
        if (lookup.ordinalMap != null && term >= 0) {
          term = lookup.getGlobalOrd(subIndex, term);
        }
        int arrIdx = term-state.startTermIndex;
        // TODO: As the arrays are always of full size and counted from 0, much of this could be skipped
        if (arrIdx>=0 && arrIdx<state.counts.size()) {
          references++;
          state.counts.inc(arrIdx);
        }
        doc = disi.nextDoc();
      }
      visitedChunks++;
      if (doc < nextChunkStart && doc != DocIdSetIterator.NO_MORE_DOCS) {
        doc = disi.advance(nextChunkStart);
      }
    }
    // TODO: Remove this when sparse faceting is considered stable
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      log.info(String.format(Locale.ENGLISH,
          "accumSingle(%d->%d) impl=%s, init=%dms, advance=%dms, " +
              "docs(samples=%d, wanted=%d, indexHits=%d, maxDoc=%d), increments=%d " +
              "(%.1f incs/doc)," +
              " incTime=%dms (%d incs/ms, %d docs/ms), " +
              "heuristic=%b (chunks=%d, factor=%f)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M,
          sampleDocs, maxSampleDocs, state.hitCount, endDocID-startDocID, references,
          sampleDocs == 0 ? 0 : 1.0*references/sampleDocs, incNS / M, incNS == 0 ? 0 : references * M / incNS,
          incNS == 0 ? 0 : sampleDocs * M / incNS,
          heuristic, maxChunks, sampleFactor));
    }
    state.refCount.addAndGet(references);
  }
  private static final long M = 1000000;

  /*
  Iterate multi-value DocValues and update the counters based on delivered ordinals.
   */
  private static void accumMulti(
      SparseState state, SortedSetDocValues ssi, DocIdSetIterator disi,
      int startDocID, int endDocID, int subIndex, Lookup lookup, long initNS, boolean heuristic) throws IOException {
    final long startTime = System.nanoTime();
    final int segmentSampleSize = state.keys.segmentSampleSize(state.hitCount, state.maxDoc, endDocID - startDocID + 1);
    final int hChunks = !heuristic ? 1 : state.keys.heuristicSampleChunks;
    final int hChunkSize = !heuristic ? endDocID-startDocID+1 : segmentSampleSize / hChunks;
    final int hChunkSkip = (endDocID-startDocID+1)/hChunks;
    if (ssi == DocValues.emptySortedSet()) {
      return; // Nothing to process; return immediately
    }
    int doc;
    doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;

    long docs = 0;
    long increments = 0;
    for (int chunk = 0 ; chunk < hChunks ; chunk++) { // heuristic
      if (doc < startDocID+chunk*hChunkSkip && doc != DocIdSetIterator.NO_MORE_DOCS) {
        doc = disi.advance(startDocID+chunk*hChunkSkip);
      }
      final int chunkEnd = Math.min(endDocID, startDocID+chunk*hChunkSkip+hChunkSize);
      while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < chunkEnd) {
        docs++;
        ssi.setDocument(doc);
        doc = disi.nextDoc();

        // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
        int term = (int) ssi.nextOrd();
        if (term < 0) {
          state.counts.incMissing();
          /*if (startTermIndex == -1) {
          counts.inc(0); // missing count
        } */
          continue;
        }

        do {
          term = lookup.getGlobalOrd(subIndex, term);
          int arrIdx = term - state.startTermIndex;

          if (arrIdx >= 0 && arrIdx < state.counts.size()) {
            increments++;
            state.counts.inc(arrIdx);
          }
        } while ((term = (int) ssi.nextOrd()) >= 0);
      }
    }
    // TODO: Remove this when sparse faceting is considered stable
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      final double sampleFactor =
          state.keys.segmentSampleFactor(state.hitCount, state.maxDoc, endDocID - startDocID + 1);
      log.info(String.format(Locale.ENGLISH,
          "accumMulti(%d->%d) impl=%s, init=%dms, advance=%dms, docHits=%d, increments=%d (%d incs/doc)," +
              " incTime=%dms (%d incs/ms, %d docs/ms), heuristic=%b (chunks=%d, chunkSize=%d, skip=%d, factor=%f)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M, docs, increments,
          docs == 0 ? 0 : increments/docs, incNS / M, incNS == 0 ? 0 : increments * M / incNS,
          incNS == 0 ? 0 : docs * M / incNS, heuristic, hChunkSize, hChunkSkip, hChunkSkip, sampleFactor));
    }
    state.refCount.addAndGet(increments);
  }

  /**
   * Slow callback with white- or blacklist of terms. This needs to resolve the String for each otherwise viable
   * candidate.
   */
  private static class PatternMatchingCallback implements ValueCounter.Callback {
    private int min;
    private final int[] maxTermCounts;
    private final boolean doNegative;
    private final LongPriorityQueue queue;
    private final int queueMaxSize;
    private final SparseKeys sparseKeys;
    private boolean isOrdered = false;
    private final SparseCounterPool pool;
    private final SortedSetDocValues si;
    private final FieldType ft;
    private final CharsRef charsRef;

    private final Matcher[] whiteMatchers;
    private final Matcher[] blackMatchers;

    /**
     * Creates a basic callback where only the values >= min are considered.
     * @param min      the starting min value.
     * @param queue   the destination of the values of the counters.
     */
    public PatternMatchingCallback(
        int min, LongPriorityQueue queue, int queueMaxSize, SparseKeys sparseKeys, SparseCounterPool pool,
        SortedSetDocValues si, FieldType ft, CharsRef charsRef) {
      this.maxTermCounts = null;
      this.min = min;
      this.doNegative = false;
      this.queue = queue;
      this.queueMaxSize = queueMaxSize;
      this.sparseKeys = sparseKeys;
      this.pool = pool;
      this.si = si;
      this.ft = ft;
      this.charsRef = charsRef;
      // Instead of generating new matchers all the time, we create them once and re-use them
      whiteMatchers = generateMatchers(sparseKeys.whitelists);
      blackMatchers = generateMatchers(sparseKeys.blacklists);
    }

    private Matcher[] generateMatchers(List<Pattern> patterns) {
      Matcher[] matchers = new Matcher[patterns.size()];
      for (int i = 0 ; i < patterns.size() ; i++) {
        matchers[i] = patterns.get(i).matcher("dummy");
      }
      return matchers;
    }

    @Override
    public void setOrdered(boolean isOrdered) {
      this.isOrdered = isOrdered;
    }

    @Override
    public final boolean handle(final int counter, final long value) {
      final int c = (int) (doNegative ? maxTermCounts[counter] - value : value);
      if (isOrdered ? c > min : c>=min) {
        // NOTE: Using > only works when the results are delivered in order.
        // The ordered uses c>min rather than c>=min as an optimization because we are going in
        // index order, so we already know that the keys are ordered.  This can be very
        // important if a lot of the counts are repeated (like zero counts would be).

        // smaller term numbers sort higher, so subtract the term number instead
        final long pair = (((long)c)<<32) + (Integer.MAX_VALUE - counter);
        //boolean displaced = queue.insert(pair);
        int regexps = 0;
        if (queue.size() < queueMaxSize || pair > queue.top()) { // Add to queue
          long patternStart = System.nanoTime();
          try {
            //final String term = resolveTerm(pool, sparseKeys, si, ft, counter-1, charsRef, br);
            final String term = resolveTerm(pool, sparseKeys, si, ft, counter, charsRef);
            for (Matcher whiteMatcher: whiteMatchers) {
              regexps++;
              whiteMatcher.reset(term);
              if (!whiteMatcher.matches()) {
                return false;
              }
            }
            for (Matcher blackMatcher: blackMatchers) {
              regexps++;
              blackMatcher.reset(term);
              if (blackMatcher.matches()) {
                return false;
              }
            }
          } finally {
            if (regexps > 0) {
              pool.regexpMatches.incRel(regexps, patternStart);
            }
          }
          if (queue.insert(pair)) {
            min=(int)(queue.top() >>> 32);
            return true;
          }
        }
      }
      return false;
    }
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

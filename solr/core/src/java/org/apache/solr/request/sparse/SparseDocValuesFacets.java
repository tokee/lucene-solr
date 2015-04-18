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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
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
  // TODO: Create daemon Threads for clean shutdown
  static final ExecutorService executor = Executors.newFixedThreadPool(20);

  private SparseDocValuesFacets() {}

  // This is a equivalent to {@link UnInvertedField#getCounts} with the extension that it also handles
  // termLists
  public static NamedList<Integer> getCounts(
      SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int minCount, boolean missing,
      String sort, String prefix, String termList, SparseKeys sparseKeys, SparseCounterPool pool) throws IOException {
    if (!sparseKeys.sparse) { // Skip sparse part completely
      return termList == null ?
          DocValuesFacets.getCounts(searcher, docs, fieldName, offset, limit, minCount, missing, sort, prefix) :
          SimpleFacets.fallbackGetListedTermCounts(searcher, null, fieldName, termList, docs);
    }
    final long fullStartTime = System.nanoTime();

    // Resolve providers of terms and ordinal-mapping
    final SchemaField schemaField = searcher.getSchema().getField(fieldName);

    final SortedSetDocValues si;  // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
    {
      if (schemaField.multiValued()) {
        si = searcher.getAtomicReader().getSortedSetDocValues(fieldName);
        if (si instanceof MultiSortedSetDocValues) {
          ordinalMap = ((MultiSortedSetDocValues)si).mapping;
        }
      } else {
        SortedDocValues single = searcher.getAtomicReader().getSortedDocValues(fieldName);
        si = single == null ? null : DocValues.singleton(single);
        if (single instanceof MultiSortedDocValues) {
          ordinalMap = ((MultiSortedDocValues)single).mapping;
        }
      }
      if (si == null) {
        return finalize(new NamedList<Integer>(), searcher, schemaField, docs, -1, missing);
      }
      if (si.getValueCount() >= Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "Currently this faceting method is limited to Integer.MAX_VALUE=" + Integer.MAX_VALUE + " unique terms");
      }
    }
    // Checks whether we can logically skip faceting
    final int hitCount = docs.size();

    // Locate start and end-position in the ordinals if a prefix is given
    // TODO: This messes up dualplane & nplane counters
    int startTermIndex, endTermIndex;
    {
      final BytesRef prefixRef;
      if (prefix == null) {
        prefixRef = null;
      } else if (prefix.length()==0) {
        prefix = null;
        prefixRef = null;
      } else {
        prefixRef = new BytesRef(prefix);
      }

      if (prefix!=null) {
        startTermIndex = (int) si.lookupTerm(prefixRef);
        if (startTermIndex<0) startTermIndex=-startTermIndex-1;
        prefixRef.append(UnicodeUtil.BIG_TERM);
        endTermIndex = (int) si.lookupTerm(prefixRef);
        assert endTermIndex < 0;
        endTermIndex = -endTermIndex-1;
      } else {
        //startTermIndex=-1;
        startTermIndex=0; // Changed to explicit missingCount
        endTermIndex=(int) si.getValueCount();
      }
    }

    final int nTerms=endTermIndex-startTermIndex;
    if (nTerms <= 0 || hitCount < minCount) {
      // Should we count this in statistics? It should be fast and is fairly independent of sparse
      return finalize(new NamedList<Integer>(), searcher, schemaField, docs, -1, missing);
    }

    // Providers ready. Check that the pool has enough information and construct a counter
    final ValueCounter counts = acquireCounter(sparseKeys, searcher, si, ordinalMap, schemaField, pool);

    // Calculate counts for all relevant terms if the counter structure is empty
    // The counter structure is always empty for single-shard searches and first phase of multi-shard searches
    // Depending on pool cache setup, it might already be full and directly usable in second phase of
    // multi-shard searches
    if (counts.getContentKey() == null) {
      if (!pool.isProbablySparse(hitCount, sparseKeys)) {
        // It is guessed that the result set will be to large to be sparse so
        // the sparse tracker is disabled up front to speed up the collection phase
        counts.disableSparseTracking();
      }

      final long sparseCollectTime = System.nanoTime();
      collectCounts(sparseKeys, searcher, docs, schemaField, ordinalMap, startTermIndex, counts);
      pool.incCollectTimeRel(sparseCollectTime);
    }

    if (termList != null) {
      // Specific terms were requested. This is used with fine-counting of facet values in distributed faceting
      try {
        return extractSpecificCounts(searcher, pool, si, fieldName, docs, counts, termList);
      } finally  {
        pool.release(counts, sparseKeys);
        pool.incTermsListTotalTimeRel(fullStartTime);
      }
    }

    //final int missingCount = startTermIndex == -1 ? (int) counts.get(0) : -1;
    final int missingCount = startTermIndex == -1 ? (int) counts.getMissing() : -1;

    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    final FieldType ft = schemaField.getType();
    final NamedList<Integer> res = new NamedList<>();
    final CharsRef charsRef = new CharsRef(10);
    if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
      int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
      maxsize = Math.min(maxsize, nTerms);
      LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

//        int min=mincount-1;  // the smallest value in the top 'N' values

      final long sparseExtractTime = System.nanoTime();
      try {
        if (counts.iterate(startTermIndex==-1?1:0, nTerms, minCount, false,
            sparseKeys.blacklists.isEmpty() && sparseKeys.whitelists.isEmpty() ?
                new ValueCounter.TopCallback(minCount-1, queue) :
                new PatternMatchingCallback(minCount-1, queue, maxsize, sparseKeys, pool, si, ft, charsRef)
        )) {
          pool.incWithinCount();
        } else {
          pool.incExceededCount();
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new ArrayIndexOutOfBoundsException(String.format(
            "Logic error: Out of bounds with startTermIndex=%d, nTerms=%d, minCount=%d and counts=%s",
            startTermIndex, nTerms, minCount, counts));
      }
      pool.incExtractTimeRel(sparseExtractTime);

      // if we are deep paging, we don't have to order the highest "offset" counts.
      int collectCount = Math.max(0, queue.size() - off);
      assert collectCount <= lim;

      final long termResolveTime = System.nanoTime();
      // the start and end indexes of our list "sorted" (starting with the highest value)
      int sortedIdxStart = queue.size() - (collectCount - 1);
      int sortedIdxEnd = queue.size() + 1;
      final long[] sorted = queue.sort(collectCount);

      BytesRef br = new BytesRef(10);
      for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
        long pair = sorted[i];
        int c = (int)(pair >>> 32);
        int tnum = Integer.MAX_VALUE - (int)pair;
        res.add(resolveTerm(pool, sparseKeys, si, ft, startTermIndex+tnum, charsRef, br), c);
/*        si.lookupOrd(startTermIndex+tnum, br);
        ft.indexedToReadable(br, charsRef);
        res.add(charsRef.toString(), c);*/
      }
      pool.incTermResolveTimeRel(termResolveTime);

    } else {
      final long termResolveTime = System.nanoTime();
      // add results in index order
      int i=(startTermIndex==-1)?1:0;
      if (minCount<=0) {
        // if mincount<=0, then we won't discard any terms and we know exactly
        // where to start.
        i+=off;
        off=0;
      }
      // TODO: Add black- & white-list
      BytesRef br = new BytesRef(10);
      for (; i<nTerms; i++) {
        int c = (int) counts.get(i);
        if (c<minCount || --off>=0) continue;
        if (--lim<0) break;
        res.add(resolveTerm(pool, sparseKeys, si, ft, startTermIndex+i, charsRef, br), c);
/*        si.lookupOrd(startTermIndex+i, br);
        ft.indexedToReadable(br, charsRef);
        res.add(charsRef.toString(), c);*/
      }
      pool.incTermResolveTimeRel(termResolveTime);
    }
    pool.release(counts, sparseKeys);

    pool.incSimpleFacetTotalTimeRel(fullStartTime);
    return finalize(res, searcher, schemaField, docs, missingCount, missing);
  }

  /*
  Iterate the leafs and update the counters based on the DocValues ordinals, adjusted to the global ordinal structure.
   */
  private static void collectCounts(
      SparseKeys sparseKeys, SolrIndexSearcher searcher, DocSet docs, SchemaField schemaField, OrdinalMap ordinalMap,
      int startTermIndex, ValueCounter counts) throws IOException {
    String fieldName = schemaField.getName();
    Filter filter = docs.getTopFilter();
    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    // The que ensures that only the given max of threads are started
    // Accumulators are responsible for removing themselves from the queue
    // after counting has finished or failed
    final BlockingQueue<Accumulator> accumulators = sparseKeys.countingThreads <= 1 ? null :
        new LinkedBlockingQueue<Accumulator>(sparseKeys.countingThreads);

    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
      if (leaf.reader().maxDoc() == 0) {
        continue;
      }

      if (accumulators == null) { // In-thread counting
        try {
          new Accumulator(leaf, 0, Integer.MAX_VALUE, sparseKeys, schemaField, ordinalMap, startTermIndex, counts,
              fieldName, filter, subIndex, null).call();
          continue;
        } catch (Exception e) {
          throw new IOException("Exception calling " + Accumulator.class.getSimpleName() + " within currentThread", e);
        }
      }

      // Multi threaded counting
      // Determine the number of parts to split the docIDspace into, taking into account the maximum number
      // of threads as well as the minimum size of a part
      final int parts = Math.max(1, Math.min(sparseKeys.countingThreads,
          leaf.reader().maxDoc() / sparseKeys.countingThreadsMinDocs));
      final int blockSize = Math.max(1, leaf.reader().maxDoc() / parts);
      //System.out.println(String.format("maxDoc=%d, parts=%d, blockSize=%d", leaf.reader().maxDoc(), parts, blockSize));
      for (int i = 0 ; i < parts ; i++) {
        Accumulator accumulator = new Accumulator(
            leaf, i*blockSize, i < parts-1 ? (i+1)*blockSize : Integer.MAX_VALUE,
            sparseKeys, schemaField, ordinalMap, startTermIndex, counts, fieldName, filter, subIndex,
            accumulators);
        try {
          accumulators.put(accumulator);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while putting new Accumulator into the queue", e);
        }
        executor.submit(accumulator);
        emptyQueue(sparseKeys, accumulators); // FIXME: This should be at the end, but that trips SparseFacetTest
      }
    }
    if (accumulators == null) {
      return;
    }
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
    private final int startDocID;
    private final int endDocID;
    private final SparseKeys sparseKeys;
    private final SchemaField schemaField;
    private final OrdinalMap ordinalMap;
    private final int startTermIndex;
    private final ValueCounter counts;
    private final String fieldName;
    private final Filter filter;
    private final int subIndex;
    private final Queue<Accumulator> accumulators;

    public Accumulator(
        AtomicReaderContext leaf, int startDocID, int endDocID, SparseKeys sparseKeys, SchemaField schemaField,
        OrdinalMap ordinalMap, int startTermIndex, ValueCounter counts, String fieldName, Filter filter, int subIndex,
        Queue<Accumulator> accumulators) {
      this.leaf = leaf;
      this.startDocID = startDocID;
      this.endDocID = endDocID;
      this.sparseKeys = sparseKeys;
      this.schemaField = schemaField;
      this.ordinalMap = ordinalMap;
      this.startTermIndex = startTermIndex;
      this.counts = counts;
      this.fieldName = fieldName;
      this.filter = filter;
      this.subIndex = subIndex;
      this.accumulators = accumulators;
    }

    @Override
    public Accumulator call() throws Exception {
      try {
        // TODO: Check if dis.bits() are available and share them instead of re-issuing a lot of searches
        DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
        DocIdSetIterator disi = null;
        if (dis != null) {
          disi = dis.iterator();
        }
        if (disi == null) {
          return this;
        }
        if (schemaField.multiValued()) {
          SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.EMPTY_SORTED_SET;
          }
          final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            // FIXME: Make a proper thread safe version here
            accumSingle(counts, startTermIndex, singleton, disi, startDocID, endDocID, subIndex, ordinalMap);
          } else {
            accumMulti(counts, startTermIndex, sub, disi, startDocID, endDocID, subIndex, ordinalMap);
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.EMPTY_SORTED;
          }
          accumSingle(counts, startTermIndex, sub, disi, startDocID, endDocID, subIndex, ordinalMap);
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
   * @param br         independent BytesRef to avoid object allocation.
   * @return an external String directly usable for constructing facet response.
   */
  private static String resolveTerm(SparseCounterPool pool, SparseKeys sparseKeys, SortedSetDocValues si,
                                    FieldType ft, int ordinal, CharsRef charsRef, BytesRef br) {
    // TODO: Check that the cache spans segments instead of just handling one
    if (sparseKeys.termLookupMaxCache > 0 && sparseKeys.termLookupMaxCache >= si.getValueCount()) {
      BytesRefArray brf = pool.getExternalTerms();
      if (brf == null) {
        // Fill term cache
        brf = new BytesRefArray(Counter.newCounter()); // TODO: Incorporate this in the overall counters
        for (int ord = 0; ord < si.getValueCount(); ord++) {
          si.lookupOrd(ord, br);
          ft.indexedToReadable(br, charsRef);
          brf.append(new BytesRef(charsRef)); // Is the object allocation avoidable?
        }
        pool.setExternalTerms(brf);
      }
      return brf.get(br, ordinal).utf8ToString();
    }
    pool.setExternalTerms(null); // Make sure the memory is freed
    // No cache, so lookup directly
    si.lookupOrd(ordinal, br);
    ft.indexedToReadable(br, charsRef);
    return charsRef.toString();
  }


  /*
   * Determines the maxOrdCount for any term in the given field by looping through all live docIDs and summing the
   * termOrds. This is needed for proper setup of {@link SparseCounterPacked}.
   * Note: This temporarily allocates an int[maxDoc]. Fortunately this happens before standard counter allocation
   * so this should not blow the heap.
   */
  private static ValueCounter acquireCounter(
      SparseKeys sparseKeys, SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap,
      SchemaField schemaField, SparseCounterPool pool) throws IOException {
    // Overall the code gets a bit muddy as we do lazy extraction of meta data about the ordinals

    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.array) {
      ensureBasic(searcher, si, globalMap, schemaField, pool);
      return pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.array);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.packed) {
      ensureBasic(searcher, si, globalMap, schemaField, pool);
      return pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.packed);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.auto) {
      ensureBasic(searcher, si, globalMap, schemaField, pool);
      return pool.usePacked(sparseKeys) ?
          pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.packed) :
          pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.array);
    }
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.dualplane) {
      ensureBasicAndHistogram(searcher, si, globalMap, schemaField, pool);
      return pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.dualplane);
    }
    // nplane is very heavy at first call: Its overflow bits needs the maximum counts for all ordinals
    // to be calculated.
    if (sparseKeys.counter == SparseKeys.COUNTER_IMPL.nplane) {
      ValueCounter vc;
      if (!pool.isInitialized() || ((vc = pool.acquire(sparseKeys, SparseKeys.COUNTER_IMPL.nplane)) == null)) {
        final int[] globOrdCount = getGlobOrdCountAndUpdatePool(searcher, si, globalMap, schemaField, pool);
        NPlaneMutable.Layout layout = NPlaneMutable.getLayout(pool.getHistogram());
        PackedInts.Mutable innerCounter = new NPlaneMutable(
            layout, new NPlaneMutable.BPVIntArrayWrapper(globOrdCount, false), NPlaneMutable.DEFAULT_IMPLEMENTATION);
        vc = new SparseCounterThreaded(SparseKeys.COUNTER_IMPL.nplane, innerCounter, pool.getMaxCountForAny(),
            sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
      }
      return vc;
    }
    throw new UnsupportedOperationException("No support yet for the " + sparseKeys.counter + " counter");
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We update the pool we synchronize on
  private static int[] getGlobOrdCountAndUpdatePool(
      SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap,
      SchemaField schemaField, SparseCounterPool pool) throws IOException {
    final int[] globOrdCount = getGlobOrdCount(searcher, si, globalMap, schemaField);
    calculateAndUpdateBasicAndHistogram(searcher, si, schemaField, pool, globOrdCount);
    return globOrdCount;
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We update the pool we synchronize on
  private static void ensureBasicAndHistogram(SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap,
                                              SchemaField schemaField, SparseCounterPool pool) throws IOException {
    synchronized (pool) {
      if (pool.getHistogram() != null && pool.isInitialized()) {
        return;
      }
      final int[] globOrdCount = getGlobOrdCount(searcher, si, globalMap, schemaField);
      calculateAndUpdateBasicAndHistogram(searcher, si, schemaField, pool, globOrdCount);
    }
  }

  private static void calculateAndUpdateBasicAndHistogram(
      SolrIndexSearcher searcher, SortedSetDocValues si, SchemaField schemaField, SparseCounterPool pool,
      int[] globOrdCount) {
    if (pool.getHistogram() != null && pool.isInitialized()) {
      return;
    }
    final long startTime = System.nanoTime();
    int maxCount = -1;
    long refCount = 0;
    final long[] histogram = new long[64];
    for (int ordCount : globOrdCount) {
      histogram[PackedInts.bitsRequired(ordCount)]++;
      refCount += ordCount;
      if (ordCount > maxCount) {
        maxCount = ordCount;
      }
    }

    log.info(String.format(Locale.ENGLISH,
        "Calculated maxCountForAny=%d for field %s with %d references to %d unique values in %dms. Histogram: %s",
        maxCount, schemaField.getName(), refCount, globOrdCount.length, (System.nanoTime() - startTime) / 1000000,
        join(histogram)));
    // +1 as everything is shifted by 1 to use index 0 as special counter
    if (!pool.isInitialized()) {
      pool.setFieldProperties((int) (si.getValueCount() + 1), maxCount, searcher.maxDoc(), refCount);
    }
    pool.setHistogram(histogram);
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

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // We update the pool we synchronize on
  private static void ensureBasic(
      SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap,
      SchemaField schemaField, SparseCounterPool pool) throws IOException {
    synchronized (pool) {
      if (pool.isInitialized()) {
        return;
      }
      final long startTime = System.nanoTime();
      final int[] globOrdCount = getGlobOrdCount(searcher, si, globalMap, schemaField);

      int maxCount = -1;
      long refCount = 0;
      for (int count : globOrdCount) {
        refCount += count;
        if (count > maxCount) { // This would be faster if we only needed maxBits (just & all the counts)
          maxCount = count;
        }
      }
      log.info(String.format(
          "Calculated maxCountForAny=%d for field %s with %d references to %d unique values in %dms",
          maxCount, schemaField.getName(), refCount, globOrdCount.length, (System.nanoTime() - startTime) / 1000000));
      // +1 as everything is shifted by 1 to use index 0 as special counter
      pool.setFieldProperties((int) (si.getValueCount() + 1), maxCount, searcher.maxDoc(), refCount);
    }
  }

  /*
  Memory and CPU-power heavy extraction of counts for all global ordinals.
  This should be called as few times as possible.
   */
  private static int[] getGlobOrdCount(SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap,
                                       SchemaField schemaField) throws IOException {
    final long startTime = System.nanoTime();
    final int[] globOrdCount = new int[(int) (si.getValueCount()+1)];
    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
      Bits live = leaf.reader().getLiveDocs();
      if (schemaField.multiValued()) {
        SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(schemaField.getName());
        if (sub == null) {
          sub = DocValues.EMPTY_SORTED_SET;
        }
        final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
        if (singleton != null) {
          for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
            if (live == null || live.get(docID)) {
              int segmentOrd = singleton.getOrd(docID);
              if (segmentOrd >= 0) {
                long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
                if (globalOrd >= 0) {
                  // Not liking the cast here, but 2^31 is de facto limit for most structures
                  globOrdCount[((int) globalOrd)]++;
                }
              }
            }
          }
        } else {
          for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
            if (live == null || live.get(docID)) {
              sub.setDocument(docID);
              // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
              int ord = (int) sub.nextOrd();
              if (ord < 0) {
                continue;
              }

              do {
                if (globalMap != null) {
                  ord = (int) globalMap.getGlobalOrd(subIndex, ord);
                }
                if (ord >= 0) {
                  globOrdCount[ord]++;
                }
              } while ((ord = (int) sub.nextOrd()) >= 0);
            }
          }
        }
      } else {
        SortedDocValues sub = leaf.reader().getSortedDocValues(schemaField.getName());
        if (sub == null) {
          sub = DocValues.EMPTY_SORTED;
        }
        for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
          if (live == null || live.get(docID)) {
            int segmentOrd = sub.getOrd(docID);
            if (segmentOrd >= 0) {
              long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
              if (globalOrd >= 0) {
                // Not liking the cast here, but 2^31 is de facto limit for most structures
                globOrdCount[((int) globalOrd)]++;
              }
            }
          }
        }
      }
    }
    log.info(String.format(Locale.ENGLISH,
        "Summed global ord count for field %s with %d unique values in %dms",
        schemaField.getName(), globOrdCount.length, (System.nanoTime() - startTime) / 1000000));
    return globOrdCount;
  }

  static int warnedOrdinal = 0;
  private static NamedList<Integer> extractSpecificCounts(
      SolrIndexSearcher searcher, SparseCounterPool pool, SortedSetDocValues si, String field, DocSet docs,
      ValueCounter counts, String termList) throws IOException {
    // TODO: Extend this to use the pool.getExternalTerms if present
    final long startTime = System.nanoTime();
    final List<String> terms = StrUtils.splitSmart(termList, ",", true);
    final FieldType ft = searcher.getSchema().getFieldType(field);

    int existingTerms = 0;
    NamedList<Integer> res = new NamedList<>();
    for (String term : terms) {
      String internal = ft.toInternal(term);
      // TODO: Check if +1 is always the case (what about startTermIndex with prefix queries?)
      long index = 1+si.lookupTerm(new BytesRef(term));
      // TODO: Remove err-out after sufficiently testing
      int count;
      if (index < 0) { // This is OK. Asking for a non-existing term is normal in distributed faceting
        count = 0;
      } else if(index >= counts.size()) {
        if (warnedOrdinal != searcher.hashCode()) {
          log.warn("DocValuesFacet.extractSpecificCounts: ordinal for " + term + " in field " + field + " was "
              + index + " but the counts only go from 0 to ordinal " + counts.size() + ". Switching to " +
              "searcher.numDocs. This warning will not be repeated until the index has been updated.");
          warnedOrdinal = searcher.hashCode();
        }
        count = searcher.numDocs(new TermQuery(new Term(field, internal)), docs);
      } else {
        count = (int) counts.get((int) index);
        existingTerms++;
      }
      res.add(term, count);
    }
    pool.incTermsListLookupRel(termList, !terms.isEmpty() ? terms.get(terms.size()-1) : "",
        existingTerms, terms.size()-existingTerms, startTime);
    return res;
  }

  // TODO: Find a better solution for caching as this map will grow with each index update
  // TODO: Do not use maxDoc as key! Couple this to the underlying reader
  private static final Map<Integer, Long> maxCounts = new HashMap<Integer, Long>();
  // This is expensive so we remember the result
  private static long getMaxCountForAnyTag(SortedSetDocValues si, int maxDoc) {
    synchronized (maxCounts) {
      if (maxCounts.containsKey(maxDoc)) {
        return maxCounts.get(maxDoc);
      }
    }
    // TODO: We assume int as max to same space. We should switch to long if maxDoc*valueCount > Integer.MAX_VALUE
    final int[] counters = new int[(int) si.getValueCount()];
    long ord;
    for (int d = 0 ; d < maxDoc ; d++) {
      si.setDocument(d);
      while ((ord = si.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        counters[((int) ord)]++;
      }
    }
    long max = 0;
    for (int i = 0 ; i < counters.length ; i++) {
      if (counters[i] > max) {
        max = counters[i];
      }
    }
    synchronized (maxCounts) {
      maxCounts.put(maxDoc, max);
    }
    return max;
  }

  /*
  Iterate single-value DocValues and update the counters based on delivered ordinals.
   */
  private static void accumSingle(ValueCounter counts, int startTermIndex, SortedDocValues si, DocIdSetIterator disi,
                                  int startDocID, int endDocID, int subIndex, OrdinalMap map) throws IOException {
    int doc = disi.nextDoc();
    if (startDocID > 0 && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < endDocID) {
      int term = si.getOrd(doc);
      if (map != null && term >= 0) {
        term = (int) map.getGlobalOrd(subIndex, term);
      }
      int arrIdx = term-startTermIndex;
      if (arrIdx>=0 && arrIdx<counts.size()) {
        counts.inc(arrIdx);
      }
      doc = disi.nextDoc();
    }
  }

  /*
  Iterate multi-value DocValues and update the counters based on delivered ordinals.
   */
  private static void accumMulti(ValueCounter counts, int startTermIndex, SortedSetDocValues ssi, DocIdSetIterator disi,
                                 int startDocID, int endDocID, int subIndex, OrdinalMap map) throws IOException {
    if (ssi == DocValues.EMPTY_SORTED_SET) {
      return; // Nothing to process; return immediately
    }
    int doc;
    doc = disi.nextDoc();
    if (startDocID > 0 && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < endDocID) {
      ssi.setDocument(doc);
      doc = disi.nextDoc();

      // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
      int term = (int) ssi.nextOrd();
      if (term < 0) {
        counts.incMissing();
        /*if (startTermIndex == -1) {
          counts.inc(0); // missing count
        } */
        continue;
      }

      do {
        if (map != null) {
          term = (int) map.getGlobalOrd(subIndex, term);
        }
        int arrIdx = term - startTermIndex;
        if (arrIdx >= 0 && arrIdx < counts.size()) counts.inc(arrIdx);
      } while ((term = (int) ssi.nextOrd()) >= 0);
    }
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

    private BytesRef br = new BytesRef(""); // To avoid re-allocation

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
            final String term = resolveTerm(pool, sparseKeys, si, ft, counter, charsRef, br);
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

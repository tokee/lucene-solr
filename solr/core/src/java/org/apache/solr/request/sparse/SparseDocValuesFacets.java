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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    SchemaField schemaField = searcher.getSchema().getField(fieldName);
    final int hitCount = docs.size();
    FieldType ft = schemaField.getType();
    NamedList<Integer> res = new NamedList<Integer>();

    final SortedSetDocValues si; // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
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
      return finalize(res, searcher, schemaField, docs, -1, missing);
    }
    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          "Currently this faceting method is limited to Integer.MAX_VALUE=" + Integer.MAX_VALUE + " unique terms");
    }

    // Providers ready. Ensure that the searcher-specific pool is correctly initialized
    if (!pool.isInitialized()) {
      initializePool(searcher, si, ordinalMap, schemaField, pool);
    }

    // Determine if the final result set is likely to be within the sparse constraints and
    // potentially fall back to standard Solr faceting if not
    final boolean probablySparse = pool.isProbablySparse(hitCount, sparseKeys);
    if (!probablySparse && sparseKeys.fallbackToBase) { // Fallback to standard
      pool.incFallbacks(pool.getNotSparseReason(hitCount, sparseKeys));

      return termList == null ?
          DocValuesFacets.getCounts(searcher, docs, fieldName, offset, limit, minCount, missing, sort, prefix) :
          SimpleFacets.fallbackGetListedTermCounts(searcher, pool, fieldName, termList, docs);
    }

    // Locate start and end-position in the ordinals if a prefix is given
    final BytesRef prefixRef;
    if (prefix == null) {
      prefixRef = null;
    } else if (prefix.length()==0) {
      prefix = null;
      prefixRef = null;
    } else {
      prefixRef = new BytesRef(prefix);
    }

    int startTermIndex, endTermIndex;
    if (prefix!=null) {
      startTermIndex = (int) si.lookupTerm(prefixRef);
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = (int) si.lookupTerm(prefixRef);
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex-1;
    } else {
      startTermIndex=-1;
      endTermIndex=(int) si.getValueCount();
    }

    final int nTerms=endTermIndex-startTermIndex;
    int missingCount = -1;
    final CharsRef charsRef = new CharsRef(10);
    if (nTerms <= 0 || hitCount < minCount) {
      // Should we count this in statistics? It should be fast and is fairly independent of sparse
      return finalize(res, searcher, schemaField, docs, missingCount, missing);
    }

    final ValueCounter counts = pool.acquire(sparseKeys);

    // Calculate counts for all relevant terms if the counter structure is empty
    // The counter structure is always empty for single-shard searches and first phase of multi-shard searches
    // Depending on pool cache setup, it might already be full and directly usable in second phase of multi-shard searches
    if (counts.getContentKey() == null) {
      if (!probablySparse) {
        // It is guessed that the result set will be to large to be sparse so
        // the sparse tracker is disabled up front to speed up the collection phase
        counts.disableSparseTracking();
      }

      final long sparseCollectTime = System.nanoTime();
      collectCounts(searcher, docs, schemaField, ordinalMap, startTermIndex, counts);
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

    if (startTermIndex == -1) {
      missingCount = (int) counts.get(0);
    }

    // IDEA: we could also maintain a count of "other"... everything that fell outside
    // of the top 'N'

    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

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

/*        for (int i=(startTermIndex==-1)?1:0; i<nTerms; i++) {
          int c = counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).

            // smaller term numbers sort higher, so subtract the term number instead
            long pair = (((long)c)<<32) + (Integer.MAX_VALUE - i);
            boolean displaced = queue.insert(pair);
            if (displaced) min=(int)(queue.top() >>> 32);
          }
        }*/

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
  private static void collectCounts(SolrIndexSearcher searcher, DocSet docs, SchemaField schemaField,
                                    OrdinalMap ordinalMap, int startTermIndex, ValueCounter counts) throws IOException {
    String fieldName = schemaField.getName();
    Filter filter = docs.getTopFilter();
    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
      DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
      DocIdSetIterator disi = null;
      if (dis != null) {
        disi = dis.iterator();
      }
      if (disi != null) {
        if (schemaField.multiValued()) {
          SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.EMPTY_SORTED_SET;
          }
          final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            accumSingle(counts, startTermIndex, singleton, disi, subIndex, ordinalMap);
          } else {
            accumMulti(counts, startTermIndex, sub, disi, subIndex, ordinalMap);
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.EMPTY_SORTED;
          }
          accumSingle(counts, startTermIndex, sub, disi, subIndex, ordinalMap);
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
  private static void initializePool(
      SolrIndexSearcher searcher, SortedSetDocValues si, OrdinalMap globalMap, SchemaField schemaField,
      SparseCounterPool pool) throws IOException {
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

    int maxCount = -1;
    long refCount = 0;
    for (int count: globOrdCount) {
      refCount += count;
      if (count > maxCount) {
        maxCount = count;
      }
    }
    log.info(String.format(
        "Calculated maxCountForAny=%d for field %s with %d references to %d unique values in %dms",
        maxCount, schemaField.getName(), refCount, globOrdCount.length, (System.nanoTime()-startTime)/1000000));
    // +1 as everything is shifted by 1 to use index 0 as special counter
    pool.setFieldProperties((int) (si.getValueCount()+1), maxCount, searcher.maxDoc(), refCount);
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
    // TODO: Find a way to avoid race condition where the max is calculated more than once
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
  static void accumSingle(ValueCounter counts, int startTermIndex, SortedDocValues si, DocIdSetIterator disi,
                          int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int term = si.getOrd(doc);
      if (map != null && term >= 0) {
        term = (int) map.getGlobalOrd(subIndex, term);
      }
      int arrIdx = term-startTermIndex;
      if (arrIdx>=0 && arrIdx<counts.size()) {
        counts.inc(arrIdx);
      }
    }
  }

  /*
  Iterate multi-value DocValues and update the counters based on delivered ordinals.
   */
  static void accumMulti(ValueCounter counts, int startTermIndex, SortedSetDocValues si, DocIdSetIterator disi,
                         int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      si.setDocument(doc);
      // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
      int term = (int) si.nextOrd();
      if (term < 0) {
        if (startTermIndex == -1) {
          counts.inc(0); // missing count
        }
        continue;
      }

      do {
        if (map != null) {
          term = (int) map.getGlobalOrd(subIndex, term);
        }
        int arrIdx = term-startTermIndex;
        if (arrIdx>=0 && arrIdx<counts.size()) counts.inc(arrIdx);
      } while ((term = (int) si.nextOrd()) >= 0);
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
            final String term = resolveTerm(pool, sparseKeys, si, ft, counter-1, charsRef, br);
            for (Pattern whitelist: sparseKeys.whitelists) {
              regexps++;
              if (!whitelist.matcher(term).matches()) {
                return false;
              }
            }
            for (Pattern blacklist: sparseKeys.blacklists) {
              regexps++;
              if (blacklist.matcher(term).matches()) {
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

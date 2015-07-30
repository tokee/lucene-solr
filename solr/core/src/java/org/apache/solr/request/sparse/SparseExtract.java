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
import java.util.Map;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.LongPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction of faceting results from a populated counter structure.
 */
public class SparseExtract {
  private static final Logger log = LoggerFactory.getLogger(SparseExtract.class);
  private static int warnedOrdinal = 0;


  public static void extractTopTerms(SparseState state) throws IOException {
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
      state.res.add(OrdinalUtils.resolveTerm(
          state.pool, state.keys, state.lookup.si, ft, state.startTermIndex + i, charsRef), c);
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
    int lim=state.limit>=0 ? state.getOverprovisionedLimit() : Integer.MAX_VALUE;
    final FieldType ft = state.schemaField.getType();
    final CharsRef charsRef = new CharsRef(10);
    state.extractTime = System.nanoTime();
    int maxsize = state.limit>0 ? state.offset+state.getOverprovisionedLimit() : Integer.MAX_VALUE-1;
    maxsize = Math.min(maxsize, state.nTerms());
    LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize, 1000), maxsize, Long.MIN_VALUE);

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
      final String term = OrdinalUtils.resolveTerm(
          state.pool, state.keys, state.lookup.si, ft, state.startTermIndex + tnum, charsRef);
      if (state.effectiveHeuristic && state.keys.heuristicFineCount) { // Need to fine-count
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
    if (state.effectiveHeuristic && state.keys.heuristicFineCount) { // Order might be off
      sortByCount(state);
    }
    state.pool.incTermResolveTimeRel(state.termResolveTime);
    state.termResolveTime = System.nanoTime()-state.termResolveTime;
  }

  private static void sortByCount(SparseState state) {
    List<SimpleFacets.CountPair<String, Integer>> entries = new ArrayList<>(state.res.size());
    for (Map.Entry<String, Integer> next : state.res) {
      entries.add(new SimpleFacets.CountPair<>(next.getKey(), next.getValue()));
    }
    Collections.sort(entries, new Comparator<SimpleFacets.CountPair<String, Integer>>() {
      @Override
      public int compare(SimpleFacets.CountPair<String, Integer> o1, SimpleFacets.CountPair<String, Integer> o2) {
        int cs = -o1.val.compareTo(o2.val);
        return cs != 0 ? cs : o1.key.compareTo(o2.key);
      }
    });
    // Trim result set based on heuristic overprovision
    if (entries.size() > state.limit) {
      entries = entries.subList(0, state.limit);
    }
    state.res.clear();
    for (SimpleFacets.CountPair<String, Integer> entry: entries) {
      state.res.add(entry.key, entry.val);
    }
  }

  public static NamedList<Integer> extractSpecificCounts(SparseState state, SortedSetDocValues si) throws IOException {
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
      } else if (state.effectiveHeuristic && state.keys.heuristicFineCount) {
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
}

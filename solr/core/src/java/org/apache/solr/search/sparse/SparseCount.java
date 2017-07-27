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
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.search.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filling of counters based on result set.
 */
public class SparseCount {
  private static final Logger log = LoggerFactory.getLogger(SparseCount.class);
  private static final long M = 1000000;

  /*
    Iterate the leafs and update the counters based on the DocValues ordinals, adjusted to the global ordinal structure.
     */
  public static void collectCounts(SparseState state) throws IOException {
    final Filter filter = state.docs.getTopFilter(); // Should be Thread safe (see PerSegmentSingleValuedFaceting)
    final List<LeafReaderContext> leaves = state.searcher.getTopReaderContext().leaves();
    // The queue ensures that only the given max of threads are started
    // Accumulators are responsible for removing themselves from the queue
    // after counting has finished or failed
    final BlockingQueue<Accumulator> accumulators = state.keys.countingThreads <= 1 ? null :
        new LinkedBlockingQueue<Accumulator>(state.keys.countingThreads);

    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      LeafReaderContext leaf = leaves.get(subIndex);
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
        SparseDocValuesFacets.executor.submit(accumulator);
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

  private static void accumSingle(
      final SparseState state, final SortedDocValues si, final DocIdSetIterator disi,
      final int startDocID, final int endDocID, final int subIndex, final TermOrdinalLookup lookup,
      // TODO: Remove initNS
      long initNS, final boolean heuristic) throws IOException {
    final long startTime = System.nanoTime();

    // TODO: Harden edge cases
    final int maxSampleDocs = heuristic ?
        state.keys.segmentSampleSize(state.hitCount, state.maxDoc, endDocID-startDocID+1) :
        endDocID-startDocID+1;
    final boolean effectiveHeuristic = maxSampleDocs < endDocID-startDocID+1;

    if (effectiveHeuristic && state.keys.heuristicSampleMode == SparseKeys.HEURISTIC_SAMPLE_MODES.whole) {
      accumSingleHeuristicWhole(state, si, disi, startDocID, endDocID, subIndex, lookup, initNS);
      return;
    }

    int doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;

    int visitedChunks = 0;
    int sampleDocs = 0;
    long references = 0;
    int chunksLeft = !heuristic ? 1 : state.keys.heuristicSampleChunks;

    while (sampleDocs < maxSampleDocs && doc != DocIdSetIterator.NO_MORE_DOCS) {
      // TODO: Avoid degenerate tail by switching to full mode or breaking when missingSamples gets very low
      final int missingSamples = maxSampleDocs - sampleDocs;
      chunksLeft = Math.max(
          1,
          Math.min(chunksLeft,
              (endDocID-doc) / state.keys.heuristicSampleChunksMinSize
          ));
      if (missingSamples > endDocID-doc) { // Badly skewed sample distribution if we get here
        chunksLeft = 1;
      }

      final int chunkSize = Math.max(state.keys.heuristicSampleChunksMinSize, missingSamples/chunksLeft);
      final int nextChunkStart = doc + (endDocID-doc-missingSamples)/chunksLeft;

      final int chunkSampleLimit = sampleDocs + chunkSize;

      final int chunkDocLimit;
      if (heuristic) {
        switch (state.keys.heuristicSampleMode) {
          case index: {
            chunkDocLimit = doc + chunkSize;
            break;
          }
          case hits: {
            chunkDocLimit = endDocID+1;
            break;
          }
          default: throw new UnsupportedOperationException(
              "The heuristic sample mode '" + state.keys.heuristicSampleMode + "' is unsupported");
        }
      } else {
        chunkDocLimit = endDocID+1;
      }

//      log.info(String.format("*** doc=%d, samples:%d->%d, chLeft=%d, chSize=%d, nextChunk=%d",
//          doc, missingSamples, sampleDocs, chunksLeft, chunkSize, nextChunkStart));

      while (sampleDocs < chunkSampleLimit && doc < chunkDocLimit && doc != DocIdSetIterator.NO_MORE_DOCS) {
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

      // Prepare next chunk
      chunksLeft--;
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
              " incTime=%dms (%d incs/ms, %d docs/ms), heuristic=%b, effectiveHeuristic=%b (chunks=%d)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M,
          sampleDocs, maxSampleDocs, state.hitCount, endDocID-startDocID, references,
          sampleDocs == 0 ? 0 : 1.0*references/sampleDocs, incNS / M, incNS == 0 ? 0 : references * M / incNS,
          incNS == 0 ? 0 : sampleDocs * M / incNS, heuristic, effectiveHeuristic, visitedChunks));
    }
    state.effectiveHeuristic |= effectiveHeuristic;
    state.refCount.addAndGet(references);
  }

  private static void accumSingleHeuristicWhole(
      final SparseState state, final SortedDocValues si, final DocIdSetIterator disi,
      final int startDocID, final int endDocID, final int subIndex, final TermOrdinalLookup lookup,
      // TODO: Remove initNS
      long initNS) throws IOException {
    final long startTime = System.nanoTime();

    final int segmentSize = endDocID-startDocID+1;
    final int rawSampleSize = state.keys.segmentRawSampleSize(state.hitCount, state.maxDoc, segmentSize);
    final int estimatedSegmentHits = (int) (1L*segmentSize*state.hitCount/state.maxDoc);
    final int every = rawSampleSize >= estimatedSegmentHits ? 1 : estimatedSegmentHits/rawSampleSize;

    int doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;

    int sampleDocs = 0;
    long references = 0;

    long fastForwardNS = 0;

    while (doc != DocIdSetIterator.NO_MORE_DOCS) {
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
      fastForwardNS -= System.nanoTime();
      for (int i = 0 ; i < every ; i++) {
        if ((doc = disi.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
      }
      fastForwardNS += System.nanoTime();
    }
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      log.info(String.format(Locale.ENGLISH,
          "accumSingleHeuristicWhole(%d->%d) impl=%s, init=%dms, advance=%dms, " +
              "docs(samples=%d, wanted=%d, indexHits=%d, maxDoc=%d), increments=%d, " +
              "(%.1f incs/doc), every=%d," +
              " incTime=%dms (%d incs/ms, %d docs/ms)," +
              "fastForward=%dms (%d docs/ms)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M,
          sampleDocs, rawSampleSize, state.hitCount, endDocID-startDocID, references,
          sampleDocs == 0 ? 0 : 1.0*references/sampleDocs, every,
          incNS / M, actsPerMS(references, incNS), actsPerMS(sampleDocs, incNS),
          fastForwardNS, actsPerMS(sampleDocs * every, fastForwardNS)));
    }
    state.effectiveHeuristic |= true;
    state.refCount.addAndGet(references);
  }

  private static void accumMulti(
      SparseState state, SortedSetDocValues ssi, DocIdSetIterator disi, int startDocID, int endDocID,
      int subIndex, TermOrdinalLookup lookup, long initNS, boolean heuristic) throws IOException {
    final long startTime = System.nanoTime();
    if (ssi == DocValues.emptySortedSet()) {
      return; // Nothing to process; return immediately
    }
    final int maxSampleDocs = heuristic ?
        state.keys.segmentSampleSize(state.hitCount, state.maxDoc, endDocID-startDocID+1) :
        endDocID-startDocID+1;
    final boolean effectiveHeuristic = maxSampleDocs < endDocID-startDocID+1;

    if (effectiveHeuristic && state.keys.heuristicSampleMode == SparseKeys.HEURISTIC_SAMPLE_MODES.whole) {
      accumMultiHeuristicWhole(state, ssi, disi, startDocID, endDocID, subIndex, lookup, initNS);
      return;
    }

    int doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;

    int visitedChunks = 0;
    int sampleDocs = 0;
    long references = 0;
    int chunksLeft = !heuristic ? 1 : state.keys.heuristicSampleChunks;

    while (sampleDocs < maxSampleDocs && doc != DocIdSetIterator.NO_MORE_DOCS) {
      // TODO: Avoid degenerate tail by switching to full mode or breaking when missingSamples gets very low
      final int missingSamples = maxSampleDocs - sampleDocs;
      chunksLeft = Math.max(
          1,
          Math.min(chunksLeft,
              (endDocID-doc) / state.keys.heuristicSampleChunksMinSize
          ));
      if (missingSamples > endDocID-doc) { // Badly skewed sample distribution if we get here
        chunksLeft = 1;
      }

      final int chunkSize = Math.max(state.keys.heuristicSampleChunksMinSize, missingSamples/chunksLeft);
      final int nextChunkStart = doc + (endDocID-doc-missingSamples)/chunksLeft;

      final int chunkSampleLimit = sampleDocs + chunkSize;

      final int chunkDocLimit;
      if (heuristic) {
        switch (state.keys.heuristicSampleMode) {
          case index: {
            chunkDocLimit = doc + chunkSize;
            break;
          }
          case hits: {
            chunkDocLimit = endDocID+1;
            break;
          }
          default: throw new UnsupportedOperationException(
              "The heuristic sample mode '" + state.keys.heuristicSampleMode + "' is unsupported");
        }
      } else {
        chunkDocLimit = endDocID+1;
      }

//      log.info(String.format("*** doc=%d, samples:%d->%d, chLeft=%d, chSize=%d, nextChunk=%d",
//          doc, missingSamples, sampleDocs, chunksLeft, chunkSize, nextChunkStart));

      while (sampleDocs < chunkSampleLimit && doc < chunkDocLimit && doc != DocIdSetIterator.NO_MORE_DOCS) {
        sampleDocs++;

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

          // TODO: As the arrays are always of full size and counted from 0, much of this could be skipped
          if (arrIdx >= 0 && arrIdx < state.counts.size()) {
            references++;
            state.counts.inc(arrIdx);
          }
        } while ((term = (int) ssi.nextOrd()) >= 0);
      }


      // Prepare next chunk
      chunksLeft--;
      visitedChunks++;
      if (doc < nextChunkStart && doc != DocIdSetIterator.NO_MORE_DOCS) {
        doc = disi.advance(nextChunkStart);
      }
    }
    // TODO: Remove this when sparse faceting is considered stable
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      log.info(String.format(Locale.ENGLISH,
          "accumMulti(%d->%d) impl=%s, init=%dms, advance=%dms, " +
              "docs(samples=%d, wanted=%d, indexHits=%d, maxDoc=%d), increments=%d " +
              "(%.1f incs/doc)," +
              " incTime=%dms (%d incs/ms, %d docs/ms), heuristic=%b, effectiveHeuristic=%b (chunks=%d)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M,
          sampleDocs, maxSampleDocs, state.hitCount, endDocID-startDocID, references,
          sampleDocs == 0 ? 0 : 1.0*references/sampleDocs, incNS / M, incNS == 0 ? 0 : references * M / incNS,
          incNS == 0 ? 0 : sampleDocs * M / incNS, heuristic, effectiveHeuristic, visitedChunks));
    }
    state.effectiveHeuristic |= effectiveHeuristic;
    state.refCount.addAndGet(references);
  }

  private static void accumMultiHeuristicWhole(
      SparseState state, SortedSetDocValues ssi, DocIdSetIterator disi, int startDocID, int endDocID,
      int subIndex, TermOrdinalLookup lookup, long initNS) throws IOException {
    final long startTime = System.nanoTime();
    if (ssi == DocValues.emptySortedSet()) {
      return; // Nothing to process; return immediately
    }
    final int segmentSize = endDocID-startDocID+1;
    final int rawSampleSize = state.keys.segmentRawSampleSize(state.hitCount, state.maxDoc, segmentSize);
    final int estimatedSegmentHits = (int) (1L*segmentSize*state.hitCount/state.maxDoc);
    final int every = rawSampleSize >= estimatedSegmentHits ? 1 : estimatedSegmentHits/rawSampleSize;

    int doc = disi.nextDoc();
    if (doc < startDocID && doc != DocIdSetIterator.NO_MORE_DOCS) {
      doc = disi.advance(startDocID);
    }
    final long advanceNS = System.nanoTime()-startTime;

    int sampleDocs = 0;
    long references = 0;

    long fastForwardNS = 0;
    while (doc != DocIdSetIterator.NO_MORE_DOCS) {
      sampleDocs++;
      ssi.setDocument(doc);

      // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
      int term = (int) ssi.nextOrd();
      if (term < 0) {
        state.counts.incMissing();
          /*if (startTermIndex == -1) {
          counts.inc(0); // missing count
        } */

        // TODO: Duplicate code is ugly but we want to track time - maybe move time to state?
        fastForwardNS -= System.nanoTime();
        for (int i = 0 ; i < every ; i++) {
          if ((doc = disi.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
        }
        fastForwardNS += System.nanoTime();
      }

      do {
        term = lookup.getGlobalOrd(subIndex, term);
        int arrIdx = term - state.startTermIndex;

        // TODO: As the arrays are always of full size and counted from 0, much of this could be skipped
        if (arrIdx >= 0 && arrIdx < state.counts.size()) {
          references++;
          state.counts.inc(arrIdx);
        }
      } while ((term = (int) ssi.nextOrd()) >= 0);

      fastForwardNS -= System.nanoTime();
      for (int i = 0 ; i < every ; i++) {
        if ((doc = disi.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
      }
      fastForwardNS += System.nanoTime();
    }
    // TODO: Remove this when sparse faceting is considered stable
    if (state.keys.logExtended) {
      final long incNS = (System.nanoTime() - startTime - advanceNS);
      log.info(String.format(Locale.ENGLISH,
          "accumMultiHeuristicWhole(%d->%d) impl=%s, init=%dms, advance=%dms, " +
              "docs(samples=%d, wanted=%d, indexHits=%d, maxDoc=%d), increments=%d, " +
              "(%.1f incs/doc), every=%d, " +
              "incTime=%dms (%d incs/ms, %d docs/ms), " +
              "fastForward=%dms (%d docs/ms)",
          startDocID, endDocID, state.keys.counter, initNS / M, advanceNS / M,
          sampleDocs, rawSampleSize, state.hitCount, endDocID-startDocID, references,
          sampleDocs == 0 ? 0 : 1.0*references/sampleDocs, every,
          incNS / M, actsPerMS(references, incNS), actsPerMS(sampleDocs, incNS),
          fastForwardNS/M, actsPerMS(sampleDocs*every, fastForwardNS)));
    }
    state.effectiveHeuristic |= true;
    state.refCount.addAndGet(references);
  }

  private static long actsPerMS(long actions, long ns) {
    return ns == 0 ? 0 : actions * M / ns;
  }

  private static class Accumulator implements Callable<Accumulator> {
    private final LeafReaderContext leaf;
    private final SparseState state;
    private final int startDocID;
    private final int endDocID;
    private final Filter filter;
    private final int subIndex;
    private final Queue<Accumulator> accumulators;
    private final boolean cloneLookup;
    private final boolean heuristic;
    private TermOrdinalLookup lookup;

    public Accumulator(
        LeafReaderContext leaf, int startDocID, int endDocID, SparseState state, Filter filter,
        int subIndex, Queue<Accumulator> accumulators, boolean multiThreadedInLeaf) {
      this.state = state;
      this.leaf = leaf;
      this.startDocID = startDocID;
      this.endDocID = endDocID;
      this.filter = filter;
      this.subIndex = subIndex;
      this.accumulators = accumulators;
      this.cloneLookup = multiThreadedInLeaf;
      // TODO: Aggregate the segment-heuristic boolean. If none are heuristic, the overall result is correct
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
            accumSingle(state, singleton, disi, startDocID, endDocID, subIndex, lookup, System.nanoTime() - startTime,
                heuristic);
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
}

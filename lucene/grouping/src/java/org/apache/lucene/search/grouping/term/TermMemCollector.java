package org.apache.lucene.search.grouping.term;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueueLong;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Grouping collector with very high emphasis on speed over memory usage.
 * The collector is specialized and only supports single-level Sort.RELEVANCE both for group and inner group sorting.
 */
public class TermMemCollector extends SimpleCollector {
  // Very conservative cache
  private final static ArrayCache scoreCache = new ArrayCache(2, 0);
  private final static ArrayCache trackerCache = new ArrayCache(2, 0);
  // If hits < ratio*maxDoc of the scoreCache is filled, use sparse iteration
  public static final double DEFAULT_SPARSE_ITERATE_RATIO = 0.1;

  // If hits > ratio*maxDoc and group.limit>1, use threaded group fill
  public static final double DEFAULT_FILL_THREAD_RATIO = 0.1;
  public static final int DEFAULT_FILL_THREAD_COUNT = 1; // Threading disabled

  private static final ExecutorService executor = new ThreadPoolExecutor(
      5, 10, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
    private int creatorCount = 0;
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("TermMemCollector_" + creatorCount++);
      t.setDaemon(true);
      return t;
    }
  });

  private final double sparseIterateRatio;
  private final double sparseClearRatio;
  private final String groupField;
  private final int numGroups;
  private final SortedDocValues si; // global ord -> term
  private final PackedInts.Reader doc2ord; // global docID -> global ord
  private final float[] scores;
  private final FixedBitSet tracker;
  private final CollapsingPriorityQueue<Long, Float, Integer> topGroups;
  private final double fillThreadRatio;
  private final int fillThreadCount;
  int totalHitCount = 0;
  final int maxDoc;

  private Scorer scorer = null;
  private int docBase = 0;

  public TermMemCollector(String groupField, int numGroups, SortedDocValues si, PackedInts.Reader doc2ord) {
    this(groupField, numGroups, si, doc2ord, DEFAULT_SPARSE_ITERATE_RATIO);
  }

  public TermMemCollector(String groupField, int numGroups, SortedDocValues si, PackedInts.Reader doc2ord,
                          double sparseRatio) {
    this(groupField, numGroups, si, doc2ord, sparseRatio, sparseRatio, 
        DEFAULT_FILL_THREAD_RATIO, DEFAULT_FILL_THREAD_COUNT);
  }

  public TermMemCollector(String groupField, int numGroups, SortedDocValues si, PackedInts.Reader doc2ord,
                          double sparseIterateRatio, double sparseClearRatio, double fillThreadRatio, 
                          int fillThreadCount) {
    this.groupField = groupField;
    this.numGroups = numGroups;
    this.si = si;
    this.doc2ord = doc2ord;
    this.fillThreadRatio = fillThreadRatio;
    this.fillThreadCount = fillThreadCount;
    
    maxDoc = doc2ord.size();
    scoreCache.setNeededLength(maxDoc);
    scores = scoreCache.getFloats();
    trackerCache.setNeededLength((maxDoc+65)/64); // +64 might also work. +65 is "just to make sure". Yes, sloppy.
    tracker = new FixedBitSet(trackerCache.getLongs(), maxDoc+1);

    topGroups = new CollapsingPriorityQueue<>(numGroups);
    this.sparseIterateRatio = sparseIterateRatio;
    this.sparseClearRatio = sparseClearRatio;
  }

  @Override
  public void collect(int segmentDocID) throws IOException {
    final int globalDocID = docBase+segmentDocID;
    scores[globalDocID] = scorer.score();
    tracker.set(globalDocID);
    if (topGroups.isCandidate(scores[globalDocID])) {
      topGroups.add(doc2ord.get(globalDocID), scores[globalDocID], globalDocID);
    }
    totalHitCount++;
  }

  /**
   * Returns top groups, starting from offset.  This may
   * return null, if no groups were collected, or if the
   * number of unique groups collected is &lt;= offset.
   *
   * @param groupOffset The offset in the collected groups
   * @param fillFields Whether to fill to {@link SearchGroup#sortValues}
   * @return top groups, starting from offset
   */
  public Collection<SearchGroup<BytesRef>> getTopGroups(int groupOffset, boolean fillFields) {

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (topGroups.size() <= groupOffset) {
      return null;
    }

    final Collection<SearchGroup<BytesRef>> result = new ArrayList<>();
    int upto = 0;
    for(CollapsingPriorityQueue.Entry entry: topGroups.getEntries()) {
      if (upto++ < groupOffset) {
        continue;
      }
      SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
      // TODO: Figure out how to generify to avoid the cast
      searchGroup.groupValue =  si.lookupOrd(((Long)entry.getKey()).intValue());
      if (fillFields) {
        searchGroup.sortValues = new Object[1];
        searchGroup.sortValues[0] = entry.getValue();
      }
      // TODO: Add docID (entry.getPayload()) to the group so that group.limit==1 is handled implicitly
      result.add(searchGroup);
    }
    //System.out.println("  return " + result.size() + " groups");
    return result;
  }

  public TopGroups<BytesRef> collectGroupDocs(int withinGroupOffset, int maxDocsPerGroup, boolean countGroupDocs) {
    if (withinGroupOffset == 0 && maxDocsPerGroup == 1) {
      return fillGroupsSingleDoc(countGroupDocs);
    }
    return fillGroupsMultiDoc(withinGroupOffset, maxDocsPerGroup);
  }

  private TopGroups<BytesRef> fillGroupsMultiDoc(int withinGroupOffset, int maxDocsPerGroup) {
    Map<Long, ScoreDocPQ> groupsWithDocs = getFilledTopGroups(withinGroupOffset, maxDocsPerGroup);
    return toTopGroups(groupsWithDocs, withinGroupOffset, maxDocsPerGroup);
  }

  private TopGroups<BytesRef> fillGroupsSingleDoc(boolean countGroupDocs) {
    Map<Long, ScoreDocPQ> groupsWithDocs = new LinkedHashMap<>(topGroups.size()); // group_ordinal -> topDocs
    for (CollapsingPriorityQueue<Long, Float, Integer>.Entry entry: topGroups.getEntries()) {
      ScoreDocPQ pq = new ScoreDocPQ(1);
      pq.insert(new FloatInt(entry.getValue(), entry.getPayload())); // We already have the top-1 docs with scores
      if (countGroupDocs) {
        pq.setInserted(0);
      }
      groupsWithDocs.put(entry.getKey(), pq);
    }
    if (countGroupDocs) {
      countGroupEntries(groupsWithDocs);
    }
    return toTopGroups(groupsWithDocs, 0, 1);
  }

  // Performs another run-through of matched groups using the tracker. A fair deal faster than getFilledTopGroups.
  // If we skip this, the limit=1 case is even faster
  private void countGroupEntries(Map<Long, ScoreDocPQ> groupsWithDocs) {
    ScoreDocPQ groupPQ;
    if (totalHitCount < maxDoc*sparseIterateRatio) { // Use sparse iteration
      int docID = -1;
      while ((docID = tracker.nextSetBit(++docID)) != DocIdSetIterator.NO_MORE_DOCS) {
        if ((groupPQ = groupsWithDocs.get(doc2ord.get(docID))) != null) {
          groupPQ.incInserted();
        }
      }
      return;
    }
    // Not sparse
    for (int docID = 0 ; docID < maxDoc ; docID++) {
      if (scores[docID] != 0.0f && (groupPQ = groupsWithDocs.get(doc2ord.get(docID))) != null) {
        groupPQ.incInserted();
      }
    }
  }

  private Map<Long, ScoreDocPQ> getFilledTopGroups(int withinGroupOffset, int maxDocsPerGroup) {
    // TODO: Add absolute minimum instead of just relative
    if (totalHitCount < fillThreadRatio*maxDoc || fillThreadCount == 1) {
      return getFilledTopGroups(withinGroupOffset, maxDocsPerGroup, 0, maxDoc+1);
    }

    List<Future<Map<Long, ScoreDocPQ>>> fillers = new ArrayList<>(fillThreadCount);
    final int chunkSize = Math.max(1, maxDoc / fillThreadCount);
    int startDocID = 0;
    for (int i = 0 ; i < fillThreadCount ; i++) {
      executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          System.out.println("Hello World");
          return null;
        }
      });
      fillers.add(executor.submit(new Filler(withinGroupOffset, maxDocsPerGroup, startDocID, startDocID+chunkSize)));
      startDocID += chunkSize;
    }
    List<Map<Long, ScoreDocPQ>> topGroupss = new ArrayList<>(fillThreadCount);
    for (Future<Map<Long, ScoreDocPQ>> filler: fillers) {
      try {
        topGroupss.add(filler.get());
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for threaded group fill", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Exception while processing threaded group fill", e);
      }
    }
    return merge(topGroupss);
  }

  private Map<Long, ScoreDocPQ> merge(List<Map<Long, ScoreDocPQ>> topGroupss) {
    Map<Long, ScoreDocPQ> merged = null;
    for (Map<Long, ScoreDocPQ> topGroups: topGroupss) {
      if (merged == null) {
        merged = topGroups;
        continue;
      }
      for (Map.Entry<Long, ScoreDocPQ> entry: topGroups.entrySet()) {
        ScoreDocPQ dest = merged.get(entry.getKey());
        if (dest == null) {
          throw new IllegalStateException("All groups should be present in all thread results for merging. " +
              "Missing group for ordinal " + entry.getKey() + " in one result");
        }
        dest.add(entry.getValue());
      }
    }
    return merged;
  }

  private class Filler implements Callable<Map<Long, ScoreDocPQ>> {
    private int withinGroupOffset;
    private int maxDocsPerGroup;
    private int startDocID;
    private int endDocID;

    public Filler(int withinGroupOffset, int maxDocsPerGroup, int startDocID, int endDocID) {
      this.withinGroupOffset = withinGroupOffset;
      this.maxDocsPerGroup = maxDocsPerGroup;
      this.startDocID = startDocID;
      this.endDocID = endDocID;
    }

    @Override
    public Map<Long, ScoreDocPQ> call() throws Exception {
      return getFilledTopGroups(withinGroupOffset, maxDocsPerGroup, startDocID, endDocID);
    }
  }

  /**
   * Takes the topGroups collected from the result set and resolves the top-X documents for each.
   * This re-uses the scores calculated from the initial collection run.
   * @param startDocID where to start scanning for non-0 scores. Inclusive.
   * @param endDocID where to stop scanning for non-0 scored. Exclusive
   */
  private Map<Long, ScoreDocPQ> getFilledTopGroups(
      int withinGroupOffset, int maxDocsPerGroup, int startDocID, int endDocID) {
    Map<Long, ScoreDocPQ> groupsWithDocs = new LinkedHashMap<>(topGroups.size()); // group_ordinal -> topDocs
    for (CollapsingPriorityQueue<Long, Float, Integer>.Entry entry: topGroups.getEntries()) {
      groupsWithDocs.put(entry.getKey(), new ScoreDocPQ(withinGroupOffset + maxDocsPerGroup));
    }

    ScoreDocPQ groupPQ;
    FloatInt filler = new FloatInt(0.0f, 0);

    if (totalHitCount < maxDoc*sparseIterateRatio) { // Use sparse iteration
      int docID = startDocID-1;
      while ((docID = tracker.nextSetBit(++docID)) < endDocID) {
        if ((groupPQ = groupsWithDocs.get(doc2ord.get(docID))) != null) {
          filler.setValues(scores[docID], docID);
          groupPQ.insert(filler);
        }
      }
      return groupsWithDocs;
    }
    // Not sparse
    for (int docID = startDocID ; docID < endDocID ; docID++) {
      final float score = scores[docID];
      if (score != 0.0f && (groupPQ = groupsWithDocs.get(doc2ord.get(docID))) != null) {
        filler.setValues(scores[docID], docID);
        groupPQ.fastInsert(filler);
        /*FloatInt removed = groupPQ.insert(filler);
        if (removed != null) {
          if (scores[docID] < removed.getFloatVal()) {
            System.out.println("Error: Inserting(" + doc2ord.get(docID) + ", " + docID + ", " + scores[docID] + ") pushed out " + removed);
          } else if (scores[docID] != removed.getFloatVal()) {
            System.out.println("Inserting(" + doc2ord.get(docID) + ", " + docID + ", " + scores[docID] + ") pushed out " + removed);
          } else {
            System.out.println("Inserting(" + doc2ord.get(docID) + ", " + docID + ", " + scores[docID] + ") skipped");
          }
        } else {
          System.out.println("Inserting(" + doc2ord.get(docID) + ", " + docID + ", " + scores[docID] + ")");
        }*/
      }
    }
    return groupsWithDocs;
  }

  // Simple format conversion
  private TopGroups<BytesRef> toTopGroups(
      Map<Long, ScoreDocPQ> groupsWithDocs, int withinGroupOffset, int maxDocsPerGroup) {

    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<BytesRef>[] groupDocsResult = (GroupDocs<BytesRef>[]) new GroupDocs[groupsWithDocs.size()];

    int totalGroupedHitCount = 0;
    float totalMaxScore = Float.MIN_VALUE;
    int groupIDX = 0;
    for (Map.Entry<Long, ScoreDocPQ> topEntry: groupsWithDocs.entrySet()) {
      final BytesRef groupName = BytesRef.deepCopyOf(si.lookupOrd(topEntry.getKey().intValue()));
      //System.out.println("Resolved group " + groupName.utf8ToString() + " from ordinal " + topEntry.getKey().intValue());
      ScoreDocPQ pq = topEntry.getValue();
      if (pq.isEmpty()) {
        throw new IllegalStateException(
            "Coding error: ScoreDocPQ is empty, which should never happen. It should always have size 1 or more");
      }

      final ScoreDoc[] scoreDocs = new ScoreDoc[pq.size()];
      final Float[] sortValues = new Float[pq.size()];
      totalGroupedHitCount += pq.getInserted();

      Iterator<FloatInt> entries = pq.getFlushingIterator(false, true);
      int scoreDocIDX = 0;
      float maxScore = Float.MIN_VALUE;
      while (entries.hasNext()) {
        FloatInt entry = entries.next();
        sortValues[scoreDocIDX] = entry.getFloatVal();
        // What about shardIndex? Is it not needed here?
        scoreDocs[scoreDocs.length-1-scoreDocIDX++] = new ScoreDoc(entry.getIntVal(), entry.getFloatVal());
        if (entry.getFloatVal() > maxScore) {
          if ((maxScore = entry.getFloatVal()) > totalMaxScore) {
            totalMaxScore = maxScore;
          }
        }
      }
      groupDocsResult[groupIDX++] = new GroupDocs<>(
          maxScore, maxScore, (int) pq.getInserted(), scoreDocs, groupName, sortValues);
    }
    SortField[] sortFields = new SortField[1];
    sortFields[0] = SortField.FIELD_SCORE;
    return new TopGroups<>(sortFields, sortFields, totalHitCount, totalGroupedHitCount, groupDocsResult, totalMaxScore);
    /**
    for(SearchGroup<?> group : groups) {
      final SearchGroupDocs<GROUP_VALUE_TYPE> groupDocs = groupMap.get(group.groupValue);
      final TopDocs topDocs = groupDocs.collector.topDocs(withinGroupOffset, maxDocsPerGroup);
      groupDocsResult[groupIDX++] = new GroupDocs<>(Float.NaN,
                                                                    topDocs.getMaxScore(),
                                                                    topDocs.totalHits,
                                                                    topDocs.scoreDocs,
                                                                    groupDocs.groupValue,
                                                                    group.sortValues);
      maxScore = Math.max(maxScore, topDocs.getMaxScore());
    }

    return new TopGroups<>(groupSort.getSort(),
                                           withinGroupSort.getSort(),
                                           totalHitCount, totalGroupedHitCount, groupDocsResult,
                                           maxScore);
       */
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
  }

  @Override
  public boolean needsScores() {
    return true;
  }

  /**
   * Frees resources. Optional, but helps very much with GC when maxDocs is above 10M.
   * @return true if the clear was sparse, false if it was full.
   */
  public boolean close() {
    final boolean sparse = totalHitCount < maxDoc*sparseClearRatio;
    if (sparse) {
      int docID = -1;
      while ((docID = tracker.nextSetBit(++docID)) != DocIdSetIterator.NO_MORE_DOCS) {
        scores[docID] = 0.0f;
      }
      scoreCache.release(scores, true);
    } else {
      scoreCache.release(scores);
    }
    // Always fill-clear the tracker bits as it is a relatively small structure (1/8 * maxDoc bytes)
    trackerCache.release(tracker.getBits());
    return sparse;
  }

  public static class ScoreDocPQ extends PriorityQueueLong<FloatInt> {

    public ScoreDocPQ(int elementCount) {
      super(elementCount);
    }

    @Override
    protected FloatInt deserialize(long block, FloatInt reuse) {
      return reuse == null ? new FloatInt(block) : reuse.setCompound(block);
    }

    @Override
    protected long serialize(FloatInt element) {
      return element.getCompound();
    }

    @Override
    public boolean lessThan(FloatInt elementA, FloatInt elementB) {
      return elementA.compareTo(elementB) < 0;
    }

    @Override
    public boolean lessThan(long elementA, long elementB) {
      return elementA < elementB;
    }

    @Override
    public boolean lessThan(FloatInt element, long block) {
      return element.getCompound() < block;
    }
  }

  public int getTotalHitCount() {
    return totalHitCount;
  }

  public static class FloatInt implements Comparable<FloatInt> {
    private long compound;

    public FloatInt(float floatVal, int intVal) {
      this.compound = (((long)Float.floatToRawIntBits(floatVal)) << 32) | intVal;
    }

    public FloatInt(long compound) {
      this.compound = compound;
    }

    public long getCompound() {
      return compound;
    }

    public FloatInt setCompound(long compound) {
      this.compound = compound;
      return this;
    }

    public void setValues(float floatVal, int intVal) {
      this.compound = (((long)Float.floatToRawIntBits(floatVal)) << 32) | intVal;
    }

    public float getFloatVal() {
      return Float.intBitsToFloat((int)(compound >>> 32));
    }

    public int getIntVal() {
      return (int)compound;
    }

    @SuppressWarnings("FloatingPointEquality")
    @Override
    public int compareTo(FloatInt o) {
      // Unfortunately compareTo return an int, so we cannot just subtract the longs
      return compound < o.compound ? -1 : compound == o.compound ? 0 : 1;
    }

    @Override
    public String toString() {
      return "FloatInt(" + getFloatVal() + ", " + getIntVal() + ")";
    }
  }
}

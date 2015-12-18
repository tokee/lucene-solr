package org.apache.lucene.search.grouping;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * SecondPassGroupingCollector is the second of two passes
 * necessary to collect grouped docs.  This pass gathers the
 * top N documents per top group computed from the
 * first pass. Concrete subclasses define what a group is and how it
 * is internally collected.
 *
 * <p>See {@link org.apache.lucene.search.grouping} for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public abstract class AbstractSecondPassGroupingCollector<GROUP_VALUE_TYPE> extends Collector {

  protected final Map<GROUP_VALUE_TYPE, SearchGroupDocs<GROUP_VALUE_TYPE>> groupMap;
  private final int maxDocsPerGroup;
  protected SearchGroupDocs<GROUP_VALUE_TYPE>[] groupDocs;
  private final Collection<SearchGroup<GROUP_VALUE_TYPE>> groups;
  private final Sort withinGroupSort;
  private final Sort groupSort;

  private int totalHitCount;
  private int totalGroupedHitCount;
  protected Scorer scorer = null;
  private float lowestScore = Float.MIN_VALUE;
  private final boolean optimizeScoreCollecting;
  private final boolean isScoreSortedWithin;

  public AbstractSecondPassGroupingCollector(Collection<SearchGroup<GROUP_VALUE_TYPE>> groups, Sort groupSort, Sort withinGroupSort,
                                             int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields,
                                             boolean optimizeScoreCollecting)
    throws IOException {

    //System.out.println("SP init");
    if (groups.size() == 0) {
      throw new IllegalArgumentException("no groups to collect (groups.size() is 0)");
    }

    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.groups = groups;
    this.maxDocsPerGroup = maxDocsPerGroup;
    this.optimizeScoreCollecting = optimizeScoreCollecting;
    this.isScoreSortedWithin = withinGroupSort == null || (
        withinGroupSort.getSort().length == 1 &&
            withinGroupSort.getSort()[0].getType()==SortField.Type.SCORE &&
            !withinGroupSort.getSort()[0].getReverse()); // TODO: We could handle reverse too
    groupMap = new HashMap<>(groups.size());

    for (SearchGroup<GROUP_VALUE_TYPE> group : groups) {
      //System.out.println("  prep group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      final TopDocsCollector<?> collector;
      if (withinGroupSort == null) {
        // Sort by score
        collector = TopScoreDocCollector.create(maxDocsPerGroup, true);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, fillSortFields, getScores, getMaxScores, true);
      }
      groupMap.put(group.groupValue,
          new SearchGroupDocs<>(group.groupValue,
              collector));
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
//    System.out.println("*** Setting scorer");
    this.scorer = scorer;
    for (SearchGroupDocs<GROUP_VALUE_TYPE> group : groupMap.values()) {
      group.collector.setScorer(scorer);
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    totalHitCount++;
//    System.out.println("*** Here " + optimizeScoreCollecting + " within=" + withinGroupSort);
    if (!isScoreSortedWithin || !optimizeScoreCollecting) {
      SearchGroupDocs<GROUP_VALUE_TYPE> group = retrieveGroup(doc);
      if (group != null) {
        totalGroupedHitCount++;
        group.collector.collect(doc);
      }
      return;
    }

    // Score-sorted groups. Avoid lookup of group value if possible
    float score = scorer.score();
    if (score < lowestScore) {
//      System.out.println("Skipped as score=" + score + " for doc=" + doc + " is < " + lowestScore);

      // FIXME: This is the optimization part, but it seems to give incorrect results in GroupingSearchTest#testScoreOptimization
      return;
    }
    SearchGroupDocs<GROUP_VALUE_TYPE> group = retrieveGroup(doc);
    if (group != null) {
      if (!group.collector.hasLowestScore()) {
        throw new IllegalStateException("Expected score keeping collector");
      }
      totalGroupedHitCount++;
      group.collector.collect(doc, score);
      float gLow = group.collector.getLowestScore();
//      System.out.println("*** Updating group with doc=" + doc + ", score=" + score + ", new lowest group score is " + gLow + " from collector " + group.collector.getClass());
      if (gLow > lowestScore) { // This group is no longer lowest. Iterate to find other lowest
//        System.out.println("*** score=" + score + ", groupLow=" + gLow + ", overallLow=" + lowestScore);
        float newMin = Float.MAX_VALUE;
        // TODO: Replace with priority queue of groups?
        for (SearchGroupDocs<GROUP_VALUE_TYPE> subGroup: groupDocs) {
          if (subGroup != null && subGroup.collector != null) { // How is this possible?
            float gMin = subGroup.collector.getLowestScore();
            if (gMin < newMin) {
              newMin = gMin;
//              System.out.println("*** new lowest score=" + score);
            }
          }
        }
        if (newMin > lowestScore) {
//          System.out.println("*** Low " + lowestScore + " -> " + newMin);
          lowestScore = newMin;
        }

      }
    }
  }

  /**
   * Returns the group the specified doc belongs to or <code>null</code> if no group could be retrieved.
   *
   * @param doc The specified doc
   * @return the group the specified doc belongs to or <code>null</code> if no group could be retrieved
   * @throws IOException If an I/O related error occurred
   */
  protected abstract SearchGroupDocs<GROUP_VALUE_TYPE> retrieveGroup(int doc) throws IOException;

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    //System.out.println("SP.setNextReader");
    for (SearchGroupDocs<GROUP_VALUE_TYPE> group : groupMap.values()) {
      group.collector.setNextReader(readerContext);
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public TopGroups<GROUP_VALUE_TYPE> getTopGroups(int withinGroupOffset) {
    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<GROUP_VALUE_TYPE>[] groupDocsResult = (GroupDocs<GROUP_VALUE_TYPE>[]) new GroupDocs[groups.size()];

    int groupIDX = 0;
    float maxScore = Float.MIN_VALUE;
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
                                           withinGroupSort == null ? null : withinGroupSort.getSort(),
                                           totalHitCount, totalGroupedHitCount, groupDocsResult,
                                           maxScore);
  }


  // TODO: merge with SearchGroup or not?
  // ad: don't need to build a new hashmap
  // disad: blows up the size of SearchGroup if we need many of them, and couples implementations
  public class SearchGroupDocs<GROUP_VALUE_TYPE> {

    public final GROUP_VALUE_TYPE groupValue;
    public final TopDocsCollector<?> collector;

    public SearchGroupDocs(GROUP_VALUE_TYPE groupValue, TopDocsCollector<?> collector) {
      this.groupValue = groupValue;
      this.collector = collector;
    }
  }
}

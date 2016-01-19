package org.apache.solr.search.grouping.collector;

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
import java.util.Collection;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Concrete implementation of {@link AbstractSecondPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link SortedDocValues}
 * to collect grouped docs.
 *
 * @lucene.experimental
 */
public class OrdinalSecondPassGroupingCollector extends AbstractSecondPassGroupingCollector<Long> {

  private SortedDocValues globalDV;
  private SortedSetDocValues globalSI; // for term lookups only
  private MultiDocValues.OrdinalMap ordinalMap = null; // Will continue being null if there is only 1 segment in the index

  private final SentinelIntSet ordSet;
  private SortedDocValues index;
  private final String groupField;
  private int segmentIndex = -1; // The reader ordinal in the top-level leaves array

  @SuppressWarnings({"unchecked", "rawtypes"})
  public OrdinalSecondPassGroupingCollector(
      SolrIndexSearcher searcher, String groupField, Collection<SearchGroup<Long>> groups, Sort groupSort,
      Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
      throws IOException {
    super(groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    ordSet = new SentinelIntSet(groupMap.size(), -2);
    this.groupField = groupField;
    groupDocs = (SearchGroupDocs<Long>[]) new SearchGroupDocs[ordSet.keys.length];

    // Look at DocValuesFacets for inspiration
    globalDV = searcher.getLeafReader().getSortedDocValues(groupField);
    globalSI = globalDV == null ? null : DocValues.singleton(globalDV);
    if (globalDV instanceof MultiDocValues.MultiSortedDocValues) {
      ordinalMap = ((MultiDocValues.MultiSortedDocValues) globalDV).mapping;
    }
  }

  /*
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    index = DocValues.getSorted(readerContext.reader(), groupField);

    // Rebuild ordSet
    ordSet.clear();
    for (SearchGroupDocs<BytesRef> group : groupMap.values()) {
//      System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
    // ReallY? First we get the ord to lookup the term, then we use the term to lookup the ord?
      int ord = group.groupValue == null ? -1 : index.lookupTerm(group.groupValue);
      if (group.groupValue == null || ord >= 0) {
        groupDocs[ordSet.put(ord)] = group;
      }
    }
  }
    */
  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    index = DocValues.getSorted(readerContext.reader(), groupField);
    segmentIndex = readerContext.ord;



    // Rebuild ordSet
    //ordSet.clear();
    if (ordSet.size() != 0) { // The ordSet does not change as we use global ordinals, so skip if it is filled
      return;
    }
    // TODO: Be sure to test with multiple segments
    for (SearchGroupDocs<Long> group : groupMap.values()) {
//      System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      long ord = group.groupValue == null ? -1 : group.groupValue;
      if (group.groupValue == null || ord >= 0) {
        groupDocs[ordSet.put((int) ord)] = group; // Ugly cast here, fixing the max group count to 2 billion
      }
    }
  }

/*  protected SearchGroupDocs<BytesRef> retrieveGroup(int doc) throws IOException {
    int slot = ordSet.find(index.getOrd(doc));
    if (slot >= 0) {
      return groupDocs[slot];
    }
    return null;
  }*/

  @Override
  protected SearchGroupDocs<Long> retrieveGroup(int doc) throws IOException {
    final int segOrd = index.getOrd(doc);
    final long globOrd = ordinalMap == null ? segOrd : ordinalMap.getGlobalOrds(segmentIndex).get(segOrd);

    int slot = ordSet.find((int) globOrd);

    if (slot >= 0) {
      return groupDocs[slot];
    }
    return null;
  }
  
  @Override
  public boolean needsScores() {
    return true; // TODO, maybe we don't?
  }

  public TopGroups<BytesRef> getTopGroupsBytesRef(int withinGroupOffset) {
    TopGroups<Long> ordGroups = getTopGroups(withinGroupOffset);
    return new TopGroups<>(
        ordGroups.groupSort, ordGroups.withinGroupSort, ordGroups.totalHitCount, ordGroups.totalGroupedHitCount,
        convertGroupDocs(ordGroups.groups), ordGroups.maxScore);
  }

  private GroupDocs<BytesRef>[] convertGroupDocs(GroupDocs<Long>[] groups) {
    if (groups == null) {
      return null;
    }
    GroupDocs<BytesRef>[] result = new TermGroupDocs[groups.length];
    for (int i = 0 ; i < groups.length ; i++) {
      result[i] = new TermGroupDocs(groups[i]);
    };
    return result;
  }

  private class TermGroupDocs extends GroupDocs<BytesRef> {
    public TermGroupDocs(GroupDocs<Long> ordGroups) {
      this(ordGroups.score, ordGroups.maxScore, ordGroups.totalHits, ordGroups.scoreDocs,
          ordGroups.groupValue == null ? null : globalSI.lookupOrd(ordGroups.groupValue),
          ordGroups.groupSortValues);
    }
    public TermGroupDocs(float score, float maxScore, int totalHits, ScoreDoc[] scoreDocs, BytesRef groupValue,
                         Object[] groupSortValues) {
      super(score, maxScore, totalHits, scoreDocs, groupValue, groupSortValues);
    }
  }
}

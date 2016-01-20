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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of {@link AbstractFirstPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link SortedDocValues}
 * to collect groups.
 *
 * @lucene.experimental
 */
public class OrdinalFirstPassGroupingCollector extends AbstractFirstPassGroupingCollector<Long> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SortedDocValues globalDV;
  private SortedSetDocValues globalSI; // for term lookups only
  private MultiDocValues.OrdinalMap ordinalMap = null; // Will continue being null if there is only 1 segment in the index
  private int segmentIndex = -1; // The reader ordinal in the top-level leaves array

  private SortedDocValues indexDV;

  private String groupField;

  /**
   * Create the first pass collector.
   *
   *  @param groupField The field used to group
   *    documents. This field must be single-valued and
   *    indexed (DocValues is used to access its value
   *    per-document).
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @throws IOException When I/O related errors occur
   */
  public OrdinalFirstPassGroupingCollector(SolrIndexSearcher searcher, String groupField, Sort groupSort, int topNGroups) throws IOException {
    super(groupSort, topNGroups);
    this.groupField = groupField;

    long initStart = System.currentTimeMillis();
    // Look at DocValuesFacets for inspiration
    globalDV = searcher.getLeafReader().getSortedDocValues(groupField);
    globalSI = globalDV == null ? null : DocValues.singleton(globalDV);
    if (globalDV instanceof MultiDocValues.MultiSortedDocValues) {
      ordinalMap = ((MultiDocValues.MultiSortedDocValues) globalDV).mapping;
    }
    log.info("Timing lazy=true. Initialized ordinal lookup structures in "
        + (System.nanoTime()-initStart)/1000000 + "ms");
  }

  // TODO: Development code. Remove when working
  protected BytesRef getDocGroupValueBytesRef(int doc) {
    Long ordinal = getDocGroupValue(doc);
    return ordinal == null ? null : globalSI.lookupOrd(ordinal);
  }

  @Override
  protected Long getDocGroupValue(int doc) {
    final int ord = indexDV.getOrd(doc);
    if (ord == -1) {
      return null;
    }
    final long globOrd = ordinalMap == null ? ord : ordinalMap.getGlobalOrds(segmentIndex).get(ord);
    return globOrd == -1 ? null : globOrd;
  }

  // TODO: Development code. Remove when working
  protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
    if (groupValue == null) {
      return null;
    } else if (reuse != null) {
      reuse.bytes = ArrayUtil.grow(reuse.bytes, groupValue.length);
      reuse.offset = 0;
      reuse.length = groupValue.length;
      System.arraycopy(groupValue.bytes, groupValue.offset, reuse.bytes, 0, groupValue.length);
      return reuse;
    } else {
      return BytesRef.deepCopyOf(groupValue);
    }
  }

  @Override
  protected Long copyDocGroupValue(Long groupValue, Long reuse) {
    return groupValue; // Longs are immutable
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    indexDV = DocValues.getSorted(readerContext.reader(), groupField);
    segmentIndex = readerContext.ord;
  }
  
  @Override
  public boolean needsScores() {
    return true; // TODO, maybe we don't?
  }

  public Collection<SearchGroup<BytesRef>> getTopGroupsTerms(int groupOffset, boolean fillFields) {
    return convertGroup(getTopGroups(groupOffset, fillFields));
  }

  // The outside expects BytesRefs
  public Collection<SearchGroup<BytesRef>> convertGroup(Collection<SearchGroup<Long>> other) {
    if (other == null) {
      return null;
    }
    final Collection<SearchGroup<BytesRef>> result = new ArrayList<>();
    for (SearchGroup<Long> otherGroup: other) {
      SearchGroup<BytesRef> group = new SearchGroup<>();
      group.groupValue = otherGroup.groupValue == null ? null :
          copyDocGroupValue(globalSI.lookupOrd(otherGroup.groupValue), null);
      group.sortValues = otherGroup.sortValues;
      result.add(group);
    }
    return result;
  }
}

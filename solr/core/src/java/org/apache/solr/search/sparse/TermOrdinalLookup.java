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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Encapsulation of DocValues and OrdinalMap. Neither are Thread safe so an instance/Thread is needed, which can
 * be accomplished by calling {@link #cloneToThread()}.
 */
public class TermOrdinalLookup {
  private final SchemaField schemaField;
  private final SolrIndexSearcher searcher;

  final SortedSetDocValues si;
  final MultiDocValues.OrdinalMap ordinalMap;

  public TermOrdinalLookup(SolrIndexSearcher searcher, SchemaField schemaField) throws IOException {
    this.schemaField = schemaField;
    this.searcher = searcher;

    String fieldName = schemaField.getName();
    if (schemaField.multiValued()) {
      si = searcher.getAtomicReader().getSortedSetDocValues(fieldName);
      ordinalMap = si instanceof MultiDocValues.MultiSortedSetDocValues ?
          ((MultiDocValues.MultiSortedSetDocValues)si).mapping :
          null;
    } else {
      SortedDocValues single = searcher.getAtomicReader().getSortedDocValues(fieldName);
      si = single == null ? null : DocValues.singleton(single);
      ordinalMap = single instanceof MultiDocValues.MultiSortedDocValues ?
          ((MultiDocValues.MultiSortedDocValues)single).mapping :
          null;
    }
    if (si != null && si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          "Currently this faceting method is limited to Integer.MAX_VALUE=" + Integer.MAX_VALUE + " unique terms");
    }
  }

  /**
   * Call this from another Thread than the one constructing this to get a Thread-local instance.
   * @return a Thread-local instance of Lookup with suitable DocValues and OrdinalMap structures.
   * @throws java.io.IOException if the cloning could not be performed.
   */
  public TermOrdinalLookup cloneToThread() throws IOException {
    return new TermOrdinalLookup(searcher, schemaField);
  }

  public int getGlobalOrd(int segmentIndex, int segmentOrdinal) {
    return ordinalMap == null ? segmentOrdinal : (int) ordinalMap.getGlobalOrds(segmentIndex).get(segmentOrdinal);
  }
}

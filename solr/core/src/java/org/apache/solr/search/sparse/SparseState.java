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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.sparse.cache.SparseCounterPool;
import org.apache.solr.search.sparse.track.ValueCounter;

/**
 * State pattern implementation for sparse faceting.
 */
public class SparseState {
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
  public final TermOrdinalLookup lookup;

  public final AtomicLong refCount = new AtomicLong(0); // Number of references from docIDs to ordinals (#increments)
  public int hitCount;
  public int startTermIndex;
  public int endTermIndex;
  public boolean heuristic;
  public boolean effectiveHeuristic = false;
  public ValueCounter counts;
  public final NamedList<Integer> res = new NamedList<>();
  public boolean optimizedExtract;

  public long hitCountTime;
  public long acquireTime;
  public long collectTime;
  public long extractTime;
  public long termResolveTime;

  SparseState(
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
    lookup = new TermOrdinalLookup(searcher, schemaField);
  }

  /**
   * @return the number of terms to consider; either all terms or limited by facet.prefix.
   */
  public int nTerms() {
    return endTermIndex-startTermIndex;
  }

  /**
   * If heuristics is in effect, the limit is adjusted with over-provisioning. Else it is returned as is.
   * @return the limit to use when extracting top-X terms by count.
   */
  public int getOverprovisionedLimit() {
    return effectiveHeuristic ?
        (int) (limit*keys.heuristicOverprovisionFactor + keys.heuristicOverprovisionConstant) :
        limit;
  }


}

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

public class SparseKeysTest extends SolrTestCaseJ4 {

  public void testHeuristicFactorIndex() {
    final int HITS = 769779;
    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_BOUNDARY, 0);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 1000000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_H, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_S, "0.5");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MODE, SparseKeys.HEURISTIC_SAMPLE_MODES.index.toString());
    params.set(SparseKeys.HEURISTIC_SAMPLE_MINFACTOR, "0.001");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MAXFACTOR, "0.95");
    SparseKeys sparseKeys = new SparseKeys("foo", params);

    assertTrue("Overall heuristics should be enabled",
        sparseKeys.useOverallHeuristic(HITS, IMAXDOC));
    assertTrue("Heuristics should be enabled for the single segment in the index",
        sparseKeys.useSegmentHeuristics(HITS, IMAXDOC, SMAXDOC));
    assertEquals("Sample size should be as expected", SMAXDOC*5/10, sparseKeys.segmentSampleSize(HITS, IMAXDOC, SMAXDOC));
  }

  public void testHeuristicFactorHits() {
    final int HITS = 769779;
    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_BOUNDARY, 0);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 1000000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_H, "0.5");
    params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_S, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MODE, SparseKeys.HEURISTIC_SAMPLE_MODES.hits.toString());
    params.set(SparseKeys.HEURISTIC_SAMPLE_MINFACTOR, "0.001");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MAXFACTOR, "0.95");
    SparseKeys sparseKeys = new SparseKeys("foo", params);

    assertTrue("Overall heuristics should be enabled",
        sparseKeys.useOverallHeuristic(HITS, IMAXDOC));
    assertTrue("Heuristics should be enabled for the single segment in the index",
        sparseKeys.useSegmentHeuristics(HITS, IMAXDOC, SMAXDOC));
    assertEquals("Sample size should be as expected", HITS*5/10, sparseKeys.segmentSampleSize(HITS, IMAXDOC, SMAXDOC));
  }

  public void testHeuristicFactorHitsEnabling() {
    final int HITS = 1199999;
    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_BOUNDARY, 0);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 1000000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_H, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.01");
    params.set(SparseKeys.HEURISTIC_SAMPLE_S, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MODE, SparseKeys.HEURISTIC_SAMPLE_MODES.hits.toString());
    params.set(SparseKeys.HEURISTIC_SAMPLE_MINFACTOR, "0.001");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MAXFACTOR, "0.95");
    SparseKeys sparseKeys = new SparseKeys("foo", params);

    assertTrue("Overall heuristics should be enabled",
        sparseKeys.useOverallHeuristic(HITS, IMAXDOC));
    assertTrue("Heuristics should be enabled for the single segment in the index",
        sparseKeys.useSegmentHeuristics(HITS, IMAXDOC, SMAXDOC));
    assertEquals("Sample size should be as expected", HITS*5/10, sparseKeys.segmentSampleSize(HITS, IMAXDOC, SMAXDOC));
  }

  public void testBoundary() {
    final int HITS = 256298430;
    final int IMAXDOC = 258189147;

    { // Exceded
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(SparseKeys.SPARSE_BOUNDARY, "0.5");
      SparseKeys sparseKeys = new SparseKeys("foo", params);
      assertFalse("Should exceed with 0.5", sparseKeys.useSparse(HITS, IMAXDOC));
    }

    { // OK
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(SparseKeys.SPARSE_BOUNDARY, "1.0");
      SparseKeys sparseKeys = new SparseKeys("foo", params);
      assertTrue("Should be within with 1.0", sparseKeys.useSparse(HITS, IMAXDOC));
    }
  }

}
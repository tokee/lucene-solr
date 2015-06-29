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

import junit.framework.TestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

public class SparseKeysTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  // Sample from logfile
  // SparseKeys{sparse=true, field='url', whitelists=[], blacklists=[], termLookup=true, termLookupMaxCache=0,
  // minTags=10000, fraction=999999.0, cutOff=2.0, maxCountsTracked=-1, counter=nplanez, countingThreads=1,
  // countingThreadsMinDocs=10000, packedLimit=24, poolSize=2, poolMaxCount=2147483647, poolMinEmpty=1,
  // skipRefinement=false, cacheToken='null', legacyShowStats=false, resetStats=false, cacheDistributed=true,
  // heuristic=true(, minDocs=100000, fraction=0, sample(, fixedSize=false, sampleSize=-1, sampleChunks=1000000,
  // sampleF=2.0, sampleC=-0.45, sampleMinFactor=0.001, sampleMaxFactor=0.5), fineCount=true, overprovisionFactor=2.0,
  // overprovisionConstant=5)}
  // accumSingle(0->258189146) impl=nplanez, init=0ms, advance=0ms, docHits=17989479, increments=17989479 (1 incs/doc),
  // incTime=2537ms (7088 incs/ms, 7088 docs/ms), heuristic=false (chunks=1, chunkSize=258189147, skip=258189147)

  public void testHeuristicFactor() {
    final int HITSLOW = 3000000;
    final int HITSHIGH = 4000000;
    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_FRACTION, 0);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 1000000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_A, "-19");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0.78");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MINFACTOR, "0.001");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MAXFACTOR, "0.5");
    SparseKeys sparseKeys = new SparseKeys("foo", params);

    assertTrue("Overall heuristics should be enabled for the small request",
        sparseKeys.useOverallHeuristic(HITSLOW, IMAXDOC));
    assertTrue("Overall heuristics should be enabled for the large request",
        sparseKeys.useOverallHeuristic(HITSHIGH, IMAXDOC));
    assertFalse("Heuristics should not be enabled for the single segment in the index for small",
        sparseKeys.useSegmentHeuristics(HITSLOW, IMAXDOC, SMAXDOC));
    assertTrue("Heuristics should be enabled for the single segment in the index for large",
        sparseKeys.useSegmentHeuristics(HITSHIGH, IMAXDOC, SMAXDOC));
  }

  public void testHeuristicFactor2() {
    final int HITS = 639951;
    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_FRACTION, 0);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 1000000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_A, "0.01");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MINFACTOR, "0.001");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MAXFACTOR, "0.95");
    SparseKeys sparseKeys = new SparseKeys("foo", params);

    assertTrue("Overall heuristics should be enabled",
        sparseKeys.useOverallHeuristic(HITS, IMAXDOC));
    assertTrue("Heuristics should be enabled for the single segment in the index",
        sparseKeys.useSegmentHeuristics(HITS, IMAXDOC, SMAXDOC));
    assertEquals("Sample size should be as expected", HITS/100, sparseKeys.segmentSampleSize(HITS, IMAXDOC, SMAXDOC));
  }

}
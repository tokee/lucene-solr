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

  public void testHeuristicFactorHitsEnabling2() {
    final int HITS = 2399155;

    /*
      SparseKeys{sparse=true, boundary=2.0, field='links', whitelists=[], blacklists=[], termLookup=true, termLookupMaxCache=0, minTags=10000, fraction=999999.0, cutOff=2.0, maxCountsTracked=-1, counter=nplanez, countingThreads=1, countingThreadsMinDocs=10000, packedLimit=24, poolSize=2, poolMaxCount=2147483647, poolMinEmpty=1, skipRefinement=false, fineCountBoundary=0.5, cacheToken='null', legacyShowStats=false, resetStats=false, cacheDistributed=true, heuristic(enabled=true, minDocs=100000, boundary=100000, sample(h=0.0, t=0.01, s=0.0, b=0, sampleMinFactor=0.001, sampleMaxFactor=0.95, sampleMode=whole, sampleChunks=100000), fineCount=true, overprovisionFactor=2.0, overprovisionConstant=5)} q=h2
INFO  - 2015-07-28 14:19:52.970; org.apache.solr.request.sparse.SparseCount; accumMulti(0->258189146) impl=nplanez, init=0ms, advance=0ms, docs(samples=2399155, wanted=258189147, indexHits=2399155, maxDoc=258189146), increments=41044379 (17.1 incs/doc), incTime=3718ms (11037 incs/ms, 645 docs/ms), heuristic=false, effectiveHeuristic=false (chunks=1)
INFO  - 2015-07-28 14:19:53.529; org.apache.solr.request.sparse.SparseDocValuesFacets; Phase 1 sparse faceting of links: method=nplanez, threads=1, hits=2399155, refs=41044379, time=4277ms (hitCount=0ms, acquire=0ms, collect=3719ms, extract=339ms (sparse=true), resolve=218ms), hits/ms=560, refs/ms=9595, heuristic(requested=true, effective=false)
INFO  - 2015-07-28 14:19:53.529; org.apache.solr.core.SolrCore; [collection1] webapp=/solr path=/select params={f.url.facet.sparse.heuristic.hitsboundary=0&f.domain.facet.sparse.heuristic.hitsboundary=1000000&facet=true&facet.sparse.heuristic.sample.t=0.01&indent=true&facet.sparse.heuristic.sample.s=0.0&facet.limit=50&facet.sparse.cutoff=2.0&facet.sparse.counter=nplanez&wt=json&facet.sparse.heuristic.sample.chunks=100000&facet.sparse.heuristic.sample.maxfactor=0.95&f.links.facet.sparse.heuristic.hitsboundary=100000&facet.sparse.heuristic.sample.h=0.0&fl=url&facet.sparse.heuristic.sample.minfactor=0.001&facet.sparse.heuristic.sample.b=0&facet.sparse.heuristic=true&facet.sparse.log.extended=true&q=h2&facet.sparse.fraction=999999&facet.field=links&facet.sparse.heuristic.sample.mode=whole} hits=2399155 status=0 QTime=4349



     */

    final int IMAXDOC = 258189147;
    final int SMAXDOC = 258189147;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SparseKeys.HEURISTIC, true);
    params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 100000);
    params.set(SparseKeys.HEURISTIC_BOUNDARY, 100000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 100000);
    params.set(SparseKeys.HEURISTIC_SAMPLE_H, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.01");
    params.set(SparseKeys.HEURISTIC_SAMPLE_S, "0.0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_B, "0");
    params.set(SparseKeys.HEURISTIC_SAMPLE_MODE, SparseKeys.HEURISTIC_SAMPLE_MODES.whole.toString());
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
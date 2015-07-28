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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Appending"})
public class SparseFacetTest extends SolrTestCaseJ4 {

  // TODO: Basically everything threaded fails. This is a regression error. Culprit is the port from 4.8 to 4.10

  @BeforeClass
  public static void beforeClass() throws Exception {
    // *_dvm_s: Multi-valued DocValues String fields
    // *_dvs_s: Single-valued DocValues String fields
    initCore("solrconfig.xml","schema-sparse.xml");
    createIndex();
  }

  static int random_commit_percent = 30;
  static void randomCommit(int percent_chance) {
    if (random().nextInt(100) <= percent_chance)
      assertU(commit());
  }

  static ArrayList<String[]> pendingDocs = new ArrayList<>();

  // committing randomly gives different looking segments each time
  static void add_doc(String... fieldsAndValues) {
    pendingDocs.add(fieldsAndValues);
  }

  static void createIndex() throws Exception {
    indexFacetValues(DOCS, 0, true);

    Collections.shuffle(pendingDocs, random());
    for (String[] doc : pendingDocs) {
      assertU(adoc(doc));
      randomCommit(random_commit_percent);
    }
    assertU(commit());
    if (random().nextInt(5) == 1) { // Random test against a fully optimized index
      assertU(optimize());
    }
  }

  public static final String SINGLE_DV_FIELD = "single_dvm_s";
  public static final String SINGLE_TEXT_FIELD = "single_s1";
  public static final String MULTI_DV_FIELD = "multi_dvm_s";
  public static final String MULTI_TEXT_FIELD = "multi_s";

  public static final String MODULO_FIELD = "mod_dvm_s";

  static final int DOCS = 100;
  static final int UNIQ_VALUES = 10;
  static final int MAX_MULTI = 10;
  static final int[] MODULOS = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 25, 50};

  static void indexFacetValues(int docs, int origo, boolean randomize) {
    List<String> values = new ArrayList<>();
    for (int docID = 0 ; docID < docs ; docID++) {
      values.clear();
      values.add("id"); values.add(origo + Integer.toString(docID));
      for (int mod: MODULOS) {
        if (mod < docID && docID % mod == 0) {
          values.add(MODULO_FIELD); values.add("mod_" + mod);
        }
      }
      values.add(SINGLE_DV_FIELD); values.add("single_dv_" + random().nextInt(UNIQ_VALUES));
      values.add(SINGLE_TEXT_FIELD); values.add("single_text_" + random().nextInt(UNIQ_VALUES));
      final int MULTIS = random().nextInt(MAX_MULTI);
      for (int i = 0 ; i < MULTIS ; i++) {
        final String val = i == 0 ? "multi_uniq_" + docID : "multi_" + random().nextInt(UNIQ_VALUES);
        values.add(MULTI_DV_FIELD); values.add(val);
        values.add(MULTI_TEXT_FIELD); values.add(val);
      }

      if (randomize) {
        add_doc(values.toArray(new String[values.size()]));
      } else {
        assertU(adoc(values.toArray(new String[values.size()])));
      }

    }
    if (!randomize) {
      assertU(commit());
    }
  }

  public void testSimpleSearch() {
    assertQ("Match all (*:*) should work",
        req("*:*"),
        "//*[@numFound='" + DOCS + "']"
    );

    assertQ("Modulo 7 search should work",
        req(MODULO_FIELD + ":mod_7"),
        "//*[@numFound='" + (DOCS / 7 - 1) + "']"
    );

  }

  public void testSingleDocValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod, SINGLE_DV_FIELD);
    }
    dumpStats();
  }

  public void testMultiDocValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod, MULTI_DV_FIELD);
    }
  }

  public void testSingleTextValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod, SINGLE_TEXT_FIELD);
    }
  }


  // Seems to fail with -Dtests.seed=98FABA64875F6ADA
  public void testMultiTextValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod, MULTI_TEXT_FIELD);
    }
  }

  public void testDualPlaneFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.dualplane, SINGLE_DV_FIELD, "single_dv_", 1);

    { // Dual fail (threading not supported)
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.dualplane.toString());
      params.set(SparseKeys.COUNTING_THREADS, 2);
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set("indent", true);
      req.setParams(params);
      try {
        getEntries(req, "int name..(single_dv_[^\"]*)");
        fail("Requesting threaded counting with dualplane should fail as it is not supported");
      } catch (Exception e) {
        // Expected
      }
    }
  }

  // Fails fairly recently with
  // ant test  -Dtestcase=SparseFacetTest -Dtests.seed=E39963CA7CB1A189 -Dtests.locale=zh_CN -Dtests.timezone=Asia/Thimphu -Dtests.file.encoding=UTF-8
  public void testMultiThreadedPackedSingleValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.packed, SINGLE_DV_FIELD, "single_dv_", 2);
  }

  public void testMultiThreadedPackedMultiValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.packed, MULTI_DV_FIELD, "multi_", 2);
  }

  public void testMultiThreadedNPlaneSingleValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplane, SINGLE_DV_FIELD, "single_dv_", 2);
  }

  public void testMultiThreadedNPlaneZSingleValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplanez, SINGLE_DV_FIELD, "single_dv_", 2);
  }

  // Failed consistably with -Dtests.seed=4AF7FD360658E93E or -Dtests.seed=B5814A333F9F734C (seems reproducible)
  public void testMultiThreadedNPlaneMultiValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplane, MULTI_DV_FIELD, "multi_", 2);
  }

  public void testSingleThreadedNPlaneZMultiValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplanez, MULTI_DV_FIELD, "multi_", 1);
  }
  public void testMultiThreadedNPlaneZMultiValueFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplanez, MULTI_DV_FIELD, "multi_", 2);
  }
           // Possible fail with -Dtests.seed=319BCB8974E7AA81
  public void testMultiThreadedNPlaneZMultiValueSparseFaceting() throws Exception {
    testFacetImplementation(SparseKeys.COUNTER_IMPL.nplanez, MULTI_DV_FIELD, "multi_", 2, "id:062", 1);
  }

  // Forces tracking of full result set in nplanez, to test for overflow at the end of the planes
  public void testFullNplanezTracker() throws Exception {
    final int LIMIT = 10;
    final String FIELD = MULTI_DV_FIELD;


    final String vanilla;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, false);
      params.set("indent", true);
      req.setParams(params);
      vanilla = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    final String packed;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.packed.toString());
      params.set("indent", true);
      req.setParams(params);
      packed = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    assertEquals("Packed should match vanilla", vanilla, packed);

    final String nplanez;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplanez.toString());
      params.set("indent", true);
      req.setParams(params);
      nplanez = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    assertEquals("Nplanez should match vanilla", vanilla, nplanez);

    final String nplanezFullTrack;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplanez.toString());
      params.set(SparseKeys.CUTOFF, Integer.MAX_VALUE);
      params.set(SparseKeys.MINTAGS, 0);
      params.set(SparseKeys.FRACTION, "1.0");
      params.set(SparseKeys.LOG_EXTENDED, "true");
      params.set("indent", true);
      req.setParams(params);
      nplanezFullTrack = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    assertEquals("Nplanez with full tracking should match vanilla", vanilla, nplanezFullTrack);
  }

  public void testHeuristic() throws Exception {
    final String FIELD = MULTI_DV_FIELD;
    final String PREFIX = "multi";
    final int LIMIT = 10;

    String sparse;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set("indent", true);
      req.setParams(params);
      sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    String heuristic;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.HEURISTIC, true);
      params.set(SparseKeys.HEURISTIC_FINECOUNT, false);
      params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 2);
      params.set(SparseKeys.HEURISTIC_BOUNDARY, "0.2");
      params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.5");
      params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 6);
      params.set("indent", true);
      req.setParams(params);
      heuristic = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    String heuristicFine;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.HEURISTIC, true);
      params.set(SparseKeys.HEURISTIC_FINECOUNT, true);
      params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 2);
      params.set(SparseKeys.HEURISTIC_BOUNDARY, "0.2");
      params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.5");
      params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 6);
      params.set("indent", true);
      req.setParams(params);
      heuristicFine = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    assertFalse("sparse and heuristic should differ but were identical", sparse.equals(heuristic));
    assertEquals("sparse and heuristic with fine count should be identical", sparse, heuristicFine);
  }

  public void testHeuristicWholeSingle() throws Exception {
    testHeuristicWhole(SINGLE_DV_FIELD);
  }
  public void testHeuristicWholeMulti() throws Exception {
    testHeuristicWhole(MULTI_DV_FIELD);
  }
  private void testHeuristicWhole(String field) throws Exception {
    final int LIMIT = 10;

    String sparse;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, field);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set("indent", true);
      req.setParams(params);
      sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    String heuristic;
    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, field);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.HEURISTIC, true);
      params.set(SparseKeys.HEURISTIC_FINECOUNT, false);
      params.set(SparseKeys.HEURISTIC_SAMPLE_CHUNKS, 2);
      params.set(SparseKeys.HEURISTIC_BOUNDARY, "0.2");
      params.set(SparseKeys.HEURISTIC_SAMPLE_T, "0.5");
      params.set(SparseKeys.HEURISTIC_SEGMENT_MINDOCS, 6);
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set(SparseKeys.HEURISTIC_SAMPLE_MODE, SparseKeys.HEURISTIC_SAMPLE_MODES.whole.toString());
      params.set("indent", true);
      req.setParams(params);
      heuristic = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    }

    assertFalse("sparse and heuristic should differ for field " + field + " but were identical",
        sparse.equals(heuristic));
  }

  public void testTermsCount() throws Exception {
    final String FIELD = MULTI_DV_FIELD;
    final String TERMS = "multi_1,multi_2";
    final String PREFIX = "multi";
    final int LIMIT = 10;

    String vanilla;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, false);
      params.set("indent", true);
      req.setParams(params);
      vanilla = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertEquals("Vanilla Solr faceting should give the expected number of results",
          TERMS.split(",").length, getEntries(req, "int name..(" + PREFIX + "[^\"]*)").size());
    }

    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.packed.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set(SparseKeys.STATS, true);
      params.set("indent", true);
      req.setParams(params);
      String sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertTrue("Requesting sparse fsior terms lookup should result in sparse processing\n" + sparse,
          sparse.contains("statistics"));
    }


    SparseKeys.COUNTER_IMPL[] COUNTERS = new SparseKeys.COUNTER_IMPL[] {
        SparseKeys.COUNTER_IMPL.array,
        SparseKeys.COUNTER_IMPL.packed,
        SparseKeys.COUNTER_IMPL.nplanez
    };
    for (double cutoff : new double[]{0.1, 1000}) {
      for (SparseKeys.COUNTER_IMPL impl : COUNTERS) {
        SolrQueryRequest req = req("*:*");
        ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
        params.set(FacetParams.FACET, true);
        params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
        params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
        params.set(FacetParams.FACET_LIMIT, LIMIT);
        params.set(SparseKeys.SPARSE, true);
        params.set(SparseKeys.CUTOFF, "" + cutoff);
        params.set(SparseKeys.COUNTER, impl.toString()); // Force sparse
        params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
        params.set(SparseKeys.LOG_EXTENDED, true);
        params.set("indent", true);
        req.setParams(params);
        String sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
        assertEquals(
            "Packed sparse faceting should give the expected number of results for " + impl + " with cutoff " + cutoff,
            TERMS.split(",").length, getEntries(req, "int name..(" + PREFIX + "[^\"]*)").size());
        assertEquals("Sparse counter implementation " + impl + " with cutoff " + cutoff + " should match vanilla",
            vanilla, sparse);
      }
    }
  }

  // Very poor unit test as it needs manual inspection of the log to verify correct result
  public void testTermsCountBoundary() throws Exception {
    final String FIELD = MULTI_DV_FIELD;
    final String TERMS = "multi_1,multi_2";
    final String PREFIX = "multi";
    final int LIMIT = 10;

    String vanilla;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, false);
      params.set("indent", true);
      req.setParams(params);
      vanilla = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertEquals("Vanilla Solr faceting should give the expected number of results",
          TERMS.split(",").length, getEntries(req, "int name..(" + PREFIX + "[^\"]*)").size());
    }

    {
      log.info("Expecting fallback=true");
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.FINECOUNT_BOUNDARY, 10);
      params.set(SparseKeys.CACHE_TOKEN, "MyCacheToken");
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.packed.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set(SparseKeys.STATS, true);
      params.set("indent", true);
      req.setParams(params);
      String above = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertTrue("Requesting sparse terms lookup should result in sparse processing\n" + above,
          above.contains("statistics"));
    }

    { // above boundary
      log.info("Expecting fallback=false");
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.FINECOUNT_BOUNDARY, "2.0");
      params.set(SparseKeys.CACHE_TOKEN, "MyCacheToken");
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.packed.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set(SparseKeys.STATS, true);
      params.set("indent", true);
      req.setParams(params);
      String below = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertTrue("Requesting sparse fsior terms lookup should result in sparse processing\n" + below,
          below.contains("statistics"));
    }

    {
      log.info("Expecting fallback=false due to cached counter");
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, String.format(Locale.ENGLISH, "{!terms=$%1$s__terms}%1$s", FIELD));
      params.set(String.format(Locale.ENGLISH, "%s__terms", FIELD), TERMS);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.FINECOUNT_BOUNDARY, 10);
      params.set(SparseKeys.CACHE_TOKEN, "MyCacheToken");
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.packed.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set(SparseKeys.LOG_EXTENDED, true);
      params.set(SparseKeys.STATS, true);
      params.set("indent", true);
      req.setParams(params);
      String above = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertTrue("Requesting sparse terms lookup should result in sparse processing\n" + above,
          above.contains("statistics"));
    }

  }

  public void testFacetImplementation(
      SparseKeys.COUNTER_IMPL implementation, String field, String resultPrefix, int threads) throws Exception {
    testFacetImplementation(implementation, field, resultPrefix, threads, "*:*", -1);
  }
    public void testFacetImplementation(SparseKeys.COUNTER_IMPL implementation, String field, String resultPrefix,
                                        int threads, String query, int expected) throws Exception {
    final int RUNS = 10; // To raise the chance of triggering a race condition bug
    final int LIMIT = 10;
    expected = expected == -1 ? LIMIT : expected;

    for (int run = 1; run <= RUNS; run++) {
      for (int minTags: new int[]{1, 1000000}) { // non-sparse, sparse
        String dry;
        { // Dry run
//          System.out.println(">>> dry run " + run);
          SolrQueryRequest req = req(query);
          ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
          params.set(FacetParams.FACET, true);
          params.set(FacetParams.FACET_FIELD, field);
          params.set(FacetParams.FACET_LIMIT, LIMIT);
          params.set(FacetParams.FACET_MINCOUNT, 1);
          params.set(SparseKeys.SPARSE, false);
          params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.array.toString());
          params.set(SparseKeys.MINTAGS, minTags);
          params.set("indent", true);
          req.setParams(params);
          dry = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
//          assertEquals("Plain sparse faceting " + " with minTags=" + minTags + " should give the expected number of "
//                  + "results in run #" + run + "\n" + dry,
//              expected, getEntries(req, "int name..(" + resultPrefix + "[^\"]*)").size());
          // The actual number of multi is random
        }

//        System.out.println(">>> single run " + run);
        String special;
        { // Special impl non-threaded
          SolrQueryRequest req = req(query);
          ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
          params.set(FacetParams.FACET, true);
          params.set(FacetParams.FACET_FIELD, field);
          params.set(FacetParams.FACET_LIMIT, LIMIT);
          params.set(FacetParams.FACET_MINCOUNT, 1);
          params.set(SparseKeys.SPARSE, true);
          params.set(SparseKeys.COUNTER, implementation.toString());
          params.set(SparseKeys.MINTAGS, minTags);
          params.set(SparseKeys.CUTOFF, 10000);
          params.set(SparseKeys.LOG_EXTENDED, true);
          params.set("indent", true);
          req.setParams(params);
          special = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
          assertEquals("The result from single threaded " + implementation + " faceting on " + field
                  + " with minTags=" + minTags + " should match expected in run " + run,
              dry, special);
        }
//        System.out.println(">>> multi " + threads + " run " + run);
        String specialT;
        if (threads > 1) { // Special impl threaded
          SolrQueryRequest req = req(query);
          ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
          params.set(FacetParams.FACET, true);
          params.set(FacetParams.FACET_FIELD, field);
          params.set(FacetParams.FACET_LIMIT, LIMIT);
          params.set(FacetParams.FACET_MINCOUNT, 1);
          params.set(SparseKeys.SPARSE, true);
          params.set(SparseKeys.COUNTER, implementation.toString());
          params.set(SparseKeys.COUNTING_THREADS, threads);
          params.set(SparseKeys.COUNTING_THREADS_MINDOCS, 2);
          params.set(SparseKeys.MINTAGS, minTags);
          params.set(SparseKeys.CUTOFF, 10000);
          params.set(SparseKeys.LOG_EXTENDED, true);
          params.set("indent", true);
//          params.set("debug", "timing");
          req.setParams(params);
          specialT = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
          assertEquals(
              "The result from " + threads + " threaded " + implementation + " faceting on " + field
                  + " with minTags=" + minTags + " should match expected in run " + run,
              dry, specialT);
        }
      }
    }
  }

  public void testBlackAndWhitelistFaceting() throws Exception {
    //dumpStats();

    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set("indent", true);
      req.setParams(params);
      assertEquals("Plain sparse faceting should give the expected number of results",
          5, getEntries(req, "int name..(single_dv_[^\"]*)").size());
    }

    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.MINTAGS, 1);
      params.set(SparseKeys.WHITELIST, "single_dv_[37]");
      params.set("indent", true);
      req.setParams(params);
      List<String> entries = getEntries(req, "int name..(single_dv_[^\"]*)");
      Collections.sort(entries);
      assertEquals("Whitelist sparse faceting should give the expected result",
          "[single_dv_3, single_dv_7]",
          entries.toString());
    }

    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.MINTAGS, 1);
      params.set(SparseKeys.BLACKLIST, "single_dv_[0-6]");
      params.set("indent", true);
      req.setParams(params);
      List<String> entries = getEntries(req, "int name..(single_dv_[^\"]*)");
      Collections.sort(entries);
      assertEquals("blacklist sparse faceting should give the expected result",
          "[single_dv_7, single_dv_8, single_dv_9]",
          entries.toString());
    }

    {
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.MINTAGS, 1);
      params.set(SparseKeys.WHITELIST, "single_dv_[37]");
      params.set(SparseKeys.BLACKLIST, "single_dv_[0-6]");
      params.set("indent", true);
      req.setParams(params);
      List<String> entries = getEntries(req, "int name..(single_dv_[^\"]*)");
      Collections.sort(entries);
      assertEquals("Combined white- & black-list sparse faceting should give the expected result",
          "[single_dv_7]",
          entries.toString());
    }
  }

  // 1-off error with -Dtests.seed=6230C3ABDB49B3EC and setting "entries = 0" in StatCollectingBPVWrapper (and reset)
  public void testOptimizedSegmentNPlaneConstructor() throws Exception {
    final String FIELD = MULTI_DV_FIELD;
    final String PREFIX = "multi";
    final int LIMIT = 10;

    assertU(optimize());

    String vanilla;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, false);
      params.set("indent", true);
      req.setParams(params);
      vanilla = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertEquals("Vanilla Solr faceting should give the expected number of results",
          LIMIT, getEntries(req, "int name..(" + PREFIX + "[^\"]*)").size());
    }

    String nplane;
    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, FIELD);
      params.set(FacetParams.FACET_LIMIT, LIMIT);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplane.toString());
      params.set(SparseKeys.MINTAGS, 1);
      params.set("indent", true);
      req.setParams(params);
      nplane = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
      assertEquals("NPlane sparse faceting should give the expected number of results",
          LIMIT, getEntries(req, "int name..(" + PREFIX + "[^\"]*)").size());
    }
    assertEquals("NPlane should match vanilla Solr", vanilla, nplane);
  }

  // disabled as it takes a long time to build an index of a size where
  // threading is beneficial (10s or 100s of millions documents)
  // TODO: Create a test with fewer documents but a large amount of references from documents to values
  public void disabledtestThreadedPerformance() throws Exception {
    final int NEW_DOCS = 10*1000;
    //final int NEW_DOCS = 1000000;
    final int WARMUPS = 5;
    final int RUNS = 10;
    final int THREADS = 3;

    indexFacetValues(NEW_DOCS, DOCS+1000, false);
    assertU(optimize());

    long totSingle = 0;
    long totSingleT = 0;
    {
      SolrQueryRequest single = req("*:*");
      ModifiableSolrParams sParams = new ModifiableSolrParams(single.getParams());
      sParams.set(FacetParams.FACET, true);
      sParams.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      sParams.set(FacetParams.FACET_LIMIT, 5);
      sParams.set(SparseKeys.SPARSE, true);
      sParams.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplane.toString());
      sParams.set(SparseKeys.MINTAGS, 1);
      single.setParams(sParams);

      SolrQueryRequest singleT = req("*:*");
      ModifiableSolrParams stParams = new ModifiableSolrParams(singleT.getParams());
      stParams.set(FacetParams.FACET, true);
      stParams.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      stParams.set(FacetParams.FACET_LIMIT, 5);
      stParams.set(SparseKeys.SPARSE, true);
      sParams.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplane.toString());
      stParams.set(SparseKeys.COUNTING_THREADS, THREADS);
      stParams.set(SparseKeys.COUNTING_THREADS_MINDOCS, 10000); // Should be way higher (10K or more)
      stParams.set(SparseKeys.MINTAGS, 1);
      singleT.setParams(stParams);

      for (int warmup = 1; warmup <= WARMUPS; warmup++) {
        assertNotNull(h.query(single));
        assertNotNull(h.query(singleT));
      }

      for (int run = 1; run <= RUNS; run++) {
        totSingle -= System.nanoTime();
        assertNotNull(h.query(single));
        totSingle += System.nanoTime();

        totSingleT -= System.nanoTime();
        assertNotNull(h.query(singleT));
        totSingleT += System.nanoTime();
      }
    }

    long totMulti = 0;
    long totMultiT = 0;
    {
      SolrQueryRequest multi = req("*:*");
      ModifiableSolrParams mParams = new ModifiableSolrParams(multi.getParams());
      mParams.set(FacetParams.FACET, true);
      mParams.set(FacetParams.FACET_FIELD, MULTI_DV_FIELD);
      mParams.set(FacetParams.FACET_LIMIT, 5);
      mParams.set(SparseKeys.SPARSE, true);
      mParams.set(SparseKeys.MINTAGS, 1);
      multi.setParams(mParams);

      SolrQueryRequest multiT = req("*:*");
      ModifiableSolrParams mtParams = new ModifiableSolrParams(multiT.getParams());
      mtParams.set(FacetParams.FACET, true);
      mtParams.set(FacetParams.FACET_FIELD, MULTI_DV_FIELD);
      mtParams.set(FacetParams.FACET_LIMIT, 5);
      mtParams.set(SparseKeys.SPARSE, true);
      mtParams.set(SparseKeys.COUNTING_THREADS, THREADS);
      mtParams.set(SparseKeys.COUNTING_THREADS_MINDOCS, 10000); // Should be way higher (10K or more)
      mtParams.set(SparseKeys.MINTAGS, 1);
      multiT.setParams(mtParams);

      for (int warmup = 1; warmup <= WARMUPS; warmup++) {
        assertNotNull(h.query(multi));
        assertNotNull(h.query(multiT));
      }

      for (int run = 1; run <= RUNS; run++) {
        totMulti -= System.nanoTime();
        assertNotNull(h.query(multi));
        totMulti += System.nanoTime();

        totMultiT -= System.nanoTime();
        assertNotNull(h.query(multiT));
        totMultiT += System.nanoTime();
      }
    }

    System.out.println(String.format(Locale.ENGLISH,
        "Average from " + RUNS + " runs: single: %3.1f ms, singleT: %3.1f ms, multi: %3.1f ms, multiT: %3.1f ms",
        1.0*totSingle/1000000/RUNS, 1.0*totSingleT/1000000/RUNS,
        1.0*totMulti/1000000/RUNS, 1.0*totMultiT/1000000/RUNS));
  }

  private List<String> getEntries(SolrQueryRequest req, String regexp) throws Exception {
    final String result = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    List<String> matches = new ArrayList<>();
    Matcher matcher = Pattern.compile(regexp).matcher(result);
    while (matcher.find()) {
      matches.add(matcher.group(1));
    }
    return matches;
  }

  private void dumpStats() throws Exception {
    SolrQueryRequest req = req("*:*");
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    req.setParams(params);
    params.set(FacetParams.FACET, true);
    params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
    params.set(CommonParams.DEBUG, CommonParams.TIMING);
//    params.set(SparseKeys.STATS, true);
    params.set("indent", true);
    String output = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    System.out.println(output);
  }

  private void assertFacetEquality(String message, String query, String facetField) throws Exception {
    SolrQueryRequest req = req(query);
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    params.set(FacetParams.FACET, true);
    params.set(FacetParams.FACET_FIELD, facetField);
    params.set("indent", true);

    params.set(SparseKeys.SPARSE, false);
    req.setParams(params);
    String plain = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");

    params.set(SparseKeys.SPARSE, true);
    SparseKeys.COUNTER_IMPL[] COUNTERS = new SparseKeys.COUNTER_IMPL[] {
        SparseKeys.COUNTER_IMPL.array,
        SparseKeys.COUNTER_IMPL.packed,
        SparseKeys.COUNTER_IMPL.nplanez
    };
    for (SparseKeys.COUNTER_IMPL counter: COUNTERS) {
      params.set(SparseKeys.COUNTER, counter.toString());
      req.setParams(params);
      String sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");

      assertEquals(message + " sparse faceting with query " + query + " and counter implementation " + counter
          + " should match plain Solr",
          plain, sparse);
    }
  }

/*    assertQ("check counts for facet queries",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.query", "trait_s:Obnoxious"
                ,"facet.query", "id:[42 TO 45]"
                ,"facet.query", "id:[43 TO 47]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=6]"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='trait_s:Obnoxious'][.='2']"
            ,"//lst[@name='facet_queries']/int[@name='id:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id:[43 TO 47]'][.='5']"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
            ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
            );
   */
}

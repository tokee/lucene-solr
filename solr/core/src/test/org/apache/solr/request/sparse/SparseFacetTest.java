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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Appending"})
public class SparseFacetTest extends SolrTestCaseJ4 {

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
    indexFacetValues();

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

  static void indexFacetValues() {

    List<String> values = new ArrayList<>();
    for (int docID = 0 ; docID < DOCS ; docID++) {
      values.clear();
      values.add("id"); values.add(Integer.toString(docID));
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
      add_doc(values.toArray(new String[values.size()]));
    }
  }

  public void testSimpleSearch() {
    assertQ("Match all (*:*) should work",
        req("*:*"),
        "//*[@numFound='" + DOCS + "']"
    );

    assertQ("Modulo 7 search should work",
        req(MODULO_FIELD + ":mod_7"),
        "//*[@numFound='" + (DOCS/7-1) + "']"
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

  public void testMultiTextValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod, MULTI_TEXT_FIELD);
    }
  }

  public void testDualPlaneFaceting() throws Exception {

    { // Dry run
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.array.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set("indent", true);
      req.setParams(params);
      assertEquals("Plain sparse faceting should give the expected number of results",
          5, getEntries(req, "int name..(single_dv_[^\"]*)").size());
    }

    { // Dual
      SolrQueryRequest req = req("*:*");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(FacetParams.FACET, true);
      params.set(FacetParams.FACET_FIELD, SINGLE_DV_FIELD);
      params.set(FacetParams.FACET_LIMIT, 5);
      params.set(SparseKeys.SPARSE, true);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.dualplane.toString());
      params.set(SparseKeys.MINTAGS, 1); // Ensure sparse
      params.set("indent", true);
      req.setParams(params);
      assertEquals("Dual plane sparse faceting should give the expected number of results",
          5, getEntries(req, "int name..(single_dv_[^\"]*)").size());
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
    req.setParams(params);
    String sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");

    assertEquals(message + " sparse faceting with query " + query + " should match plain Solr",
        plain, sparse);
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

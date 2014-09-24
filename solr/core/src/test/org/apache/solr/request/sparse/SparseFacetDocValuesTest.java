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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Appending"})
public class SparseFacetDocValuesTest extends SolrTestCaseJ4 {

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
  }

  public static final String SINGLE_FIELD = "single_dvm_s";
  public static final String MULTI_FIELD = "multi_dvm_s";
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
      values.add(SINGLE_FIELD); values.add("single_" + random().nextInt(UNIQ_VALUES));
      final int MULTIS = random().nextInt(MAX_MULTI);
      for (int i = 0 ; i < MULTIS ; i++) {
        values.add(MULTI_FIELD); values.add(
            i == 0 ?
                "multi_uniq_" + docID :
                "multi_" + random().nextInt(UNIQ_VALUES));
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

  public void testSingleValueFaceting() throws Exception {
    for (int mod: MODULOS) {
      assertFacetEquality("Modulo check", MODULO_FIELD + ":mod_" + mod);
    }
  }

  private void assertFacetEquality(String message, String query) throws Exception {
    SolrQueryRequest req = req(query);
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    params.set(FacetParams.FACET, true);
    params.set(FacetParams.FACET_FIELD, SINGLE_FIELD);
    params.set("indent", true);

    params.set(SparseKeys.SPARSE, false);
    req.setParams(params);
    String plain = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");
    params.set(SparseKeys.SPARSE, true);
    req.setParams(params);
    String sparse = h.query(req).replaceAll("QTime\">[0-9]+", "QTime\">");

    System.out.println(plain);
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

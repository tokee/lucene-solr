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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.ChaosMonkey;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

// We need SortedDoc multi value DocValues
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Appending"})
public class SparseFacetDistribTest extends AbstractFullDistribZkTestBase {
  private boolean initPerformed = false;

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema-sparse.xml";      // we need a string id
  }

  public SparseFacetDistribTest() {
    super();
    fixShardCount = true;
    shardCount = 3;
    sliceCount = 2;
  }

  // We override this to avoid the chaos monkey as it sometimes turns off DocValues support
  @Override
  protected void initCloud() throws Exception {
    if (initPerformed) {
      return;
    }
    try {
      cloudClient = createCloudClient(DEFAULT_COLLECTION);
      cloudClient.connect();
      initPerformed = true;
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void doTest() throws Exception {
    final String FF = "foo_dvm_s";
    final String THIN = "thin_dvm_s";
    final int DOCS = 1000;

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    // Add a bunch of documents with a mix of shared and unique values in the facet field
    List<String> fields = new ArrayList<>();
    for (int docID = 1 ; docID <= DOCS ; docID++) {
      fields.clear();
      fields.add("id"); fields.add(Integer.toString(docID));
      fields.add(FF);   fields.add("unique" + docID);
      fields.add(FF);   fields.add("alldocs");

      fields.add(THIN); fields.add("thin" + random().nextInt(DOCS/2));
      int extra = random().nextInt(10);
      for (int i = 0 ; i < extra ; i++) {
        fields.add(FF); fields.add("extra" + random().nextInt(100));
      }

      String[] fieldsS = new String[fields.size()];
      fields.toArray(fieldsS);
      index(fieldsS);
    }
    commit();

    // Trigger a two-phase distributed faceting call
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    for (int clientID = 0 ; clientID < shardCount ; clientID++) {
      QueryResponse results = clients.get(clientID).query(params);
      assertEquals(DOCS, results.getResults().getNumFound());
    }

    params.add(FacetParams.FACET, Boolean.TRUE.toString());
    params.add(FacetParams.FACET_FIELD, FF);
    params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
    params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
    params.set(SparseKeys.SPARSE, Boolean.FALSE.toString());

    QueryResponse nonSparse = clients.get(0).query(params);

    params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());
    params.add(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
    params.add(SparseKeys.TERMLOOKUP, Boolean.TRUE.toString());
    params.add(SparseKeys.MINTAGS, Integer.toString(0));   // Force sparse
    params.add(SparseKeys.FRACTION, Double.toString(1000.0)); // Force sparse
    params.add(SparseKeys.CUTOFF, Double.toString(2.0));   // Force sparse

    QueryResponse results = clients.get(0).query(params);

    // Comparison-tests are bad as they need the old implementation to stick around
    assertEquals("Solr fc and sparse results should be equal except for QTime",
        nonSparse.toString().replaceAll("QTime=[0-9]+", ""), results.toString().replaceAll("QTime=[0-9]+", ""));

//    System.out.println("***" + results.toString().replace(",", ",\n"));

    // Do not work with THIN as the result is random there
//    assertEquals("Count for alldocs should be #docs", DOCS, results.getFacetField(FF).getValues().get(0).getCount());

    // Check that the call was sparse-processed
    params.remove(SparseKeys.STATS_RESET);
    params.add(SparseKeys.STATS, Boolean.TRUE.toString()); // Enable sparse statistics
    results = clients.get(0).query(params);
    //System.out.println("***2 " + results.toString().replace(",", ",\n"));
    // Do we even sparse and does stats work?
    assertTrue("For sparse faceting there should an instance of 'exceededCutoff=0'\n" + results,
        results.toString().contains("exceededCutoff=0"));
    // Was all calls within the cutoff?
    assertFalse("For sparse faceting there should be no instances of 'exceededCutoff=[1-9]'\n" + results,
        results.toString().matches(".*exceededCutoff=[1-9].*"));

    // Check that fine-counting is also sparse-counted
    params.set(FacetParams.FACET_FIELD, THIN);
    params.set(FacetParams.FACET_LIMIT, Integer.toString(5));

    // We need to do this twice to get dist stats to bubble up
    clients.get(0).query(params);
    results = clients.get(0).query(params);
//    System.out.println("***3 " + results.toString().replace(",", ",\n"));

    // terms(count=0 means that the secondary fine-counting of facets was not done sparsely
    assertFalse("With fine-counting there should be no instances of 'terms(count=0'\n" + results,
        results.toString().contains("terms(count=0"));
    System.out.println(results.toString().replace(",", ",\n"));

    // Disable fine-counting
    params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
    params.set(SparseKeys.SKIPREFINEMENTS, Boolean.TRUE.toString());
    clients.get(0).query(params);
    params.remove(SparseKeys.STATS_RESET);
    results = clients.get(0).query(params);
    assertTrue("Without fine-counting there should be an instance of 'terms(count=0'\n" + results,
        results.toString().contains("terms(count=0"));
//    System.out.println("***4 " + results.toString().replace(",", ",\n"));

    // minCount 0 vs. minCount 1, both should still be sparse
    params.set(CommonParams.Q, "id:1");
    params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
    params.remove(SparseKeys.SKIPREFINEMENTS);
    params.set(SparseKeys.MAXMINCOUNT, Integer.toString(0));
    clients.get(0).query(params);
    params.remove(SparseKeys.STATS_RESET);
    results = clients.get(0).query(params);
    // With tiny result sets all possible facet values are returned in the first call, so no secondary call is needed
    assertTrue("For single-hit search, setting maxMin=0 should contain 'terms(count=0'\n" + results,
        results.toString().contains("terms(count=0"));
    assertTrue("For single-hit search, setting maxMin=0 should contain empty facet values\n" + results,
        results.toString().matches(".*thin[0-9]+=0.*"));

    /*   TODO: Add facet content with 12+ unique values and a matching search that contains zero values
    params.set(SparseKeys.MAXMINCOUNT, Integer.toString(1));
    params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
    clients.get(0).query(params);
    params.remove(SparseKeys.STATS_RESET);
    results = clients.get(0).query(params);
    // With tiny result sets all possible facet values are returned in the first call, so no secondary call is needed
    assertFalse("For single-hit search, setting maxMin=1 should not contain 'terms(count=0'\n" + results,
        results.toString().contains("terms(count=0"));
    assertFalse("For single-hit search, setting maxMin=1 should not contain empty facet values\n" + results,
        results.toString().matches(".*thin[0-9]+=0.*"));
        */

  }
}

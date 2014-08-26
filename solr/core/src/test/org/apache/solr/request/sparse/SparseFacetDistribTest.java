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
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

// We need SortedDoc multi value DocValues
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40"})
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

    params.add(SparseKeys.SPARSE, Boolean.TRUE.toString());
    params.add(SparseKeys.TERMLOOKUP, Boolean.TRUE.toString());
    params.add(SparseKeys.MINTAGS, Integer.toString(0));   // Force sparse
    params.add(SparseKeys.FRACTION, Double.toString(1.0)); // Force sparse
    params.add(SparseKeys.CUTOFF, Double.toString(2.0));   // Force sparse

    QueryResponse results = clients.get(0).query(params);
//    System.out.println(results);
    assertEquals("Count for alldocs should be #docs", DOCS, results.getFacetField(FF).getValues().get(0).getCount());
    // TODO: Add proper test for second phase counting instead of just seeing if the query completes
  }
}

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
    {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      for (int clientID = 0 ; clientID < shardCount ; clientID++) {
        QueryResponse results = clients.get(clientID).query(params);
        assertEquals("Initiating searches on different Solr's should give the same number of hits",
            DOCS, results.getResults().getNumFound());
      }
    }

    QueryResponse nonSparseFF;
    {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_FIELD, FF);
      params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.FALSE.toString());
      params.add("indent", "true");

      { // vanilla Solr FF
        nonSparseFF = clients.get(0).query(params);
        QueryResponse nonSparseFF2 = clients.get(0).query(params);
        assertEquals("Repeating the facet request with standard Solr should be equal except for QTime",
            nonSparseFF.toString().replaceAll("QTime=[0-9]+", ""),
            nonSparseFF2.toString().replaceAll("QTime=[0-9]+", ""));
      }

    }
    QueryResponse nonSparseTHIN;
    { // vanilla Solr THIN
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.set(FacetParams.FACET_FIELD, THIN);
      params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.FALSE.toString());

      nonSparseTHIN = clients.get(0).query(params);
      assertTrue("Faceting on " + THIN + " with vanilla Solr should give facet results\n"
          + nonSparseTHIN.toString().replace("{", "\n{"),
          nonSparseTHIN.toString().matches("(?m).*thin[0-9]+=[0-9]+.*"));
      params.set(FacetParams.FACET_FIELD, FF); // Change back
    }

    { // sparse equality with vanilla Solr
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.set(FacetParams.FACET_FIELD, THIN);
      params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.add(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      params.add(SparseKeys.TERMLOOKUP, Boolean.TRUE.toString());

      // TODO: Remove these when error has been isolated
//      params.set(SparseKeys.MAXMINCOUNT, Integer.toString(0)); // Let sparse behave like non-sparse
      params.set(SparseKeys.CACHE_DISTRIBUTED, Boolean.FALSE.toString()); // Caching seems to give incorrect results
      // TOD: TERMLOOKUP seems the likely culprit for failing test. Maybe it has something to do with bigterms?
      params.set(SparseKeys.TERMLOOKUP, Boolean.FALSE.toString()); // Third attempt to isolate the problem

      params.add(SparseKeys.MINTAGS, Integer.toString(0));   // Force sparse
      params.add(SparseKeys.FRACTION, Double.toString(1000.0)); // Force sparse
      params.add(SparseKeys.CUTOFF, Double.toString(2.0));   // Force sparse

      {
        params.set(FacetParams.FACET_FIELD, THIN); // Change back
        QueryResponse results = clients.get(0).query(params);
        // Comparison-tests are bad as they need the old implementation to stick around
        assertEquals("Solr fc and sparse results should be equal for " + THIN + " except for QTime\n"
            + results.toString().replace("{", "\n{"),
            nonSparseTHIN.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"),
            results.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"));
      }
      {
        params.set(FacetParams.FACET_FIELD, FF); // Change back
        QueryResponse results = clients.get(0).query(params);
        // Comparison-tests are bad as they need the old implementation to stick around
        assertEquals("Solr fc and sparse results should be equal for " + FF + " except for QTime\n"
            + results.toString().replace("{", "\n{"),
            nonSparseFF.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"),
            results.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"));
      }
    }

    { // is sparse activated?
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_FIELD, FF);
      params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      // Check that the call was sparse-processed
      params.remove(SparseKeys.STATS_RESET);
      params.add(SparseKeys.STATS, Boolean.TRUE.toString()); // Enable sparse statistics
      QueryResponse results = clients.get(0).query(params);
      // Do we even sparse and does stats work?
      assertTrue("For sparse faceting there should an instance of 'exceededCutoff=0'\n" + results,
          results.toString().contains("exceededCutoff=0"));
      // Was all calls within the cutoff?
      assertFalse("For sparse faceting there should be no instances of 'exceededCutoff=[1-9]'\n" + results,
          results.toString().matches("(?m).*exceededCutoff=[1-9].*"));
    }

    {
      // Check that fine-counting is also sparse-counted
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_LIMIT, Integer.toString(10));
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.add(SparseKeys.STATS, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, THIN);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(5));

      // We need to do this twice to get dist stats to bubble up
      clients.get(0).query(params);
      QueryResponse sparseThin = clients.get(0).query(params);

      // terms(count=0 means that the secondary fine-counting of facets was not done sparsely
      assertFalse("With fine-counting there should be no instances of 'fallback=0'\n" + sparseThin,
          sparseThin.toString().contains("terms(fallback=0"));
      assertTrue("Cache status should be available\n" + sparseThin,
          sparseThin.toString().matches("(?m).*cache.hits=.*"));
      assertTrue("At least one of the requests should hit the cache\n" + sparseThin,
          sparseThin.toString().matches("(?m).*cache.hits=[1234].*"));
    }

    // Is it possible to turn the distributed facet call cache off?
    {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, THIN);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(5));

      params.set(SparseKeys.CACHE_DISTRIBUTED, Boolean.FALSE.toString());
      params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      clients.get(0).query(params);
      params.remove(SparseKeys.STATS_RESET);

      params.add(SparseKeys.STATS, Boolean.TRUE.toString());
      clients.get(0).query(params);
      QueryResponse results = clients.get(0).query(params);
      assertTrue("With disabled cache, stats should contain neither cache hits or misses\n" + results,
          results.toString().contains("cache(hits=0, misses=0"));
      params.set(SparseKeys.CACHE_DISTRIBUTED, Boolean.TRUE.toString());
    }

    System.out.println(nonSparseFF.toString().replace("{", "\n{"));

    {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, THIN);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(5));
      params.set(SparseKeys.SKIPREFINEMENTS, Boolean.TRUE.toString());

      // Disable fine-counting
      params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      clients.get(0).query(params);
      params.remove(SparseKeys.STATS_RESET);
      params.set(SparseKeys.STATS, Boolean.TRUE.toString());

      QueryResponse sparseThin = clients.get(0).query(params);
      System.out.println(sparseThin.toString().replace("{", "\n{"));
      assertTrue("Without fine-counting stats should contain an instance of 'termLookup(calls=0)'\n"
          + sparseThin.toString().replace("{", "\n{"),
          sparseThin.toString().contains("termLookup(calls=0)"));
      params.remove(SparseKeys.SKIPREFINEMENTS);
    }

    {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "id:1");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, THIN);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(5));
      params.set(SparseKeys.SKIPREFINEMENTS, Boolean.TRUE.toString());

      // minCount 0 vs. minCount 1, both should still be sparse
      params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      params.set(SparseKeys.MAXMINCOUNT, Integer.toString(0));
      clients.get(0).query(params);
      params.remove(SparseKeys.STATS_RESET);
      params.set(SparseKeys.STATS, Boolean.TRUE.toString());
      QueryResponse sparseThin = clients.get(0).query(params);
      // With tiny result sets all possible facet values are returned in the first call, so no secondary call is needed
      assertTrue("For single-hit search, setting maxMin=0 should contain empty facet values\n"
          + sparseThin.toString().replace("{", "\n{"),
          sparseThin.toString().matches("(?m).*thin[0-9]+=0.*"));
      params.remove(SparseKeys.MAXMINCOUNT);
    }

    { // nplane fails with -Dtests.seed=A04379F5D396357C
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, FF);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(10));

      params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      clients.get(0).query(params);
      params.remove(SparseKeys.STATS_RESET);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplane.toString());

      {
        params.set(SparseKeys.STATS, Boolean.TRUE.toString());
        clients.get(0).query(params); // Double tap to get stats to bubble up
        QueryResponse ngramThin = clients.get(0).query(params);
        assertTrue("nplane faceting should be active\n"
            + ngramThin.toString().replace("{", "\n{").replace(")", ")\n"),
            ngramThin.toString().contains("nplaneAllocation(calls=1"));
      }

      {
        params.set(SparseKeys.STATS, Boolean.FALSE.toString()); // For comparison
        QueryResponse nplaneFF = clients.get(0).query(params);
        // With tiny result sets all possible facet values are returned in the first call, so no secondary call is needed
        assertEquals("nplane faceting should give the same result as Vanilla Solr\n"
            + nplaneFF.toString().replace("{", "\n{"),
            nonSparseFF.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"),
            nplaneFF.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"));
      }
    }

    { // nplanez
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.add(FacetParams.FACET, Boolean.TRUE.toString());
      params.add(FacetParams.FACET_METHOD, FacetParams.FACET_METHOD_fc);
      params.set(SparseKeys.SPARSE, Boolean.TRUE.toString());

      params.set(FacetParams.FACET_FIELD, FF);
      params.set(FacetParams.FACET_LIMIT, Integer.toString(10));

      params.set(SparseKeys.STATS_RESET, Boolean.TRUE.toString());
      clients.get(0).query(params);
      params.remove(SparseKeys.STATS_RESET);
      params.set(SparseKeys.COUNTER, SparseKeys.COUNTER_IMPL.nplanez.toString());

      {
        params.set(SparseKeys.STATS, Boolean.TRUE.toString());
        clients.get(0).query(params); // Double tap to get stats to bubble up
        QueryResponse ngramThin = clients.get(0).query(params);
        assertTrue("nplanez faceting should be active\n"
            + ngramThin.toString().replace("{", "\n{").replace(")", ")\n"),
            ngramThin.toString().contains("nplaneAllocation(calls=1"));
      }

      {
        params.set(SparseKeys.STATS, Boolean.FALSE.toString()); // For comparison
        QueryResponse nplaneFF = clients.get(0).query(params);
        // With tiny result sets all possible facet values are returned in the first call, so no secondary call is needed
        assertEquals("nplanez faceting should give the same result as Vanilla Solr\n"
            + nplaneFF.toString().replace("{", "\n{"),
            nonSparseFF.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"),
            nplaneFF.toString().replaceAll("QTime=[0-9]+", "").replace("{", "\n{"));
      }
    }
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

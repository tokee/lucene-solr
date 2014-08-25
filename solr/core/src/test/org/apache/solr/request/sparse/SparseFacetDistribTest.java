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

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

public class SparseFacetDistribTest extends AbstractFullDistribZkTestBase {

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
  }

  public SparseFacetDistribTest() {
    super();
    fixShardCount = true;
    shardCount = 3;
    sliceCount = 2;
  }

  @Override
  public void doTest() throws Exception {
    final int DOCS = 100;
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    // add a doc, search for it

    for (int docID = 1 ; docID < DOCS ; docID++) {
      indexr("id", Integer.toString(docID), "a_t", "originalcontent");
    }
    commit();


    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "a_t:originalcontent");
    for (int clientID = 0 ; clientID < shardCount ; clientID++) {
      QueryResponse results = clients.get(clientID).query(params);
      assertEquals(DOCS, results.getResults().getNumFound());
    }
  }
}

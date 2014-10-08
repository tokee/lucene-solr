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

package org.apache.solr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.sparse.SparseKeys;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
public class TestRandomFaceting extends SolrTestCaseJ4 {

  public static final String FOO_STRING_FIELD = "foo_s1";
  public static final String SMALL_STRING_FIELD = "small_s1";
  public static final String SMALL_INT_FIELD = "small_i";

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }

  private void toggleSparse() {
    if (random().nextBoolean()) {
      System.out.println("Tweaking defaults for sparse faceting to ensure usage");
      SparseKeys.SPARSE_DEFAULT = true;
      SparseKeys.MINTAGS_DEFAULT = 1;
      SparseKeys.CUTOFF_DEFAULT = 9999;
    }
  }

  int indexSize;
  List<FldType> types;
  Map<Comparable, Doc> model = null;
  boolean validateResponses = true;

  void init() {
    Random rand = random();
    clearIndex();
    model = null;
    indexSize = rand.nextBoolean() ? (rand.nextInt(10) + 1) : (rand.nextInt(100) + 10);

    types = new ArrayList<>();
    types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
    types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));
    types.add(new FldType("small_f",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("small_d",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("foo_i",ZERO_ONE, new IRange(-2,indexSize)));
    types.add(new FldType("rare_s1",new IValsPercent(95,0,5,1), new SVal('a','b',1,5)));
    types.add(new FldType("str_s1",ZERO_ONE, new SVal('a','z',1,2)));
    types.add(new FldType("long_s1",ZERO_ONE, new SVal('a','b',1,5)));
    types.add(new FldType("small_s1",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_s1",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_ss",ZERO_TWO, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small3_ss",new IRange(0,25), new SVal('A','z',1,1)));
    types.add(new FldType("small_i",ZERO_ONE, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small2_i",ZERO_ONE, new IRange(-1,5+indexSize/3)));
    types.add(new FldType("small2_is",ZERO_TWO, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small3_is",new IRange(0,25), new IRange(-50,50)));

    types.add(new FldType("missing_i",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_is",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_s1",new IRange(0,0), new SVal('a','b',1,1)));
    types.add(new FldType("missing_ss",new IRange(0,0), new SVal('a','b',1,1)));

    toggleSparse();
    // TODO: doubles, multi-floats, ints with precisionStep>0, booleans
  }

  void addMoreDocs(int ndocs) throws Exception {
    model = indexDocs(types, model, ndocs);
  }

  void deleteSomeDocs() {
    Random rand = random();
    int percent = rand.nextInt(100);
    if (model == null) return;
    ArrayList<String> ids = new ArrayList<>(model.size());
    for (Comparable id : model.keySet()) {
      if (rand.nextInt(100) < percent) {
        ids.add(id.toString());
      }
    }
    if (ids.size() == 0) return;

    StringBuilder sb = new StringBuilder("id:(");
    for (String id : ids) {
      sb.append(id).append(' ');
      model.remove(id);
    }
    sb.append(')');

    assertU(delQ(sb.toString()));

    if (rand.nextInt(10)==0) {
      assertU(optimize());
    } else {
      assertU(commit("softCommit",""+(rand.nextInt(10)!=0)));
    }
  }

  public void testDocValuesSparseFacetingCounts() {
    //final String UP = "uniqueTerm://";
    final String FF = "myfaceta_i_s_dv";
    String[] FV = new String[]{"A", "B", "A", "B", "A", "C"};
    for (int docID = 0 ; docID < FV.length ; docID++) {
      assertU(adoc("id", Integer.toString(20000 + docID), FF, FV[docID]));
    }
    assertU(commit());
    final String pre = "//lst[@name='" + FF + "']";

    // First phase non-sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "false"
            ,"facet.field", FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
        ,pre+"/int[1][@name='A'][.='3']"
        ,pre+"/int[2][@name='B'][.='2']"
        ,pre+"/int[3][@name='C'][.='1']"
    );

    // First phase sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            , "facet.sparse", "true"
            , "facet.sparse.mintags", "1" // Force sparse
            , "facet.sparse.cutoff", "99999" // Force sparse
            , "facet.sparse.termlookup", "true" // Force sparse
            ,"facet.field", FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
        ,pre+"/int[1][@name='A'][.='3']"
        ,pre+"/int[2][@name='B'][.='2']"
        ,pre+"/int[3][@name='C'][.='1']"
    );

    // Second phase non-sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "false"
            ,"facet.field", "{!terms=A}" + FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
        ,pre+"/int[1][@name='A'][.='3']"
    );

    // Second phase sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            , "facet.sparse", "true"
            , "facet.sparse.mintags", "1" // Force sparse
            , "facet.sparse.cutoff", "99999" // Force sparse
            , "facet.sparse.termlookup", "true" // Force sparse
            ,"facet.field", "{!terms=A,B}" + FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
        ,pre+"/int[1][@name='A'][.='3']"
        ,pre+"/int[2][@name='B'][.='2']"
    );
  }

  public void testDocValuesSparseFaceting() {
    //final String UP = "uniqueTerm://";
    final String UP = "uniqueTerm";
    final String FF = "myfacetb_i_s_dv";
    final int DOCS = 1000;
    for (int i = 0 ; i < DOCS ; i++) {
      // *_s = multi string, *_s1 = single string
      assertU(adoc("id", Integer.toString(10000 + i), FF, UP + i));
    }
    assertU(commit());

    final String pre = "//lst[@name='" + FF + "']";
    // First phase non-sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "false"
            ,"facet.field", FF
            ,"facet.mincount","1"
        )
        ,pre+"/int[1][@name='" + UP + "0'][.='1']"
        ,pre+"/int[2][@name='" + UP + "1'][.='1']"
    );

    // First phase sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            , "facet", "true"
            , "facet.sparse", "true"
            , "facet.sparse.mintags", "1" // Force sparse
            , "facet.sparse.cutoff", "99999" // Force sparse
            , "facet.sparse.termlookup", "true" // Force sparse
            , "facet.field", FF
            , "facet.mincount", "1"
        )
        ,pre + "/int[1][@name='" + UP + "0'][.='1']"
        , pre + "/int[2][@name='" + UP + "1'][.='1']"
    );

    // Second phase non-sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "false"
            ,"facet.field", "{!terms=" + UP + "" + (DOCS-1) + "," + UP + "0}" + FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
        ,pre+"/int[1][@name='" + UP + "999'][.='1']"
        ,pre+"/int[2][@name='" + UP + "0'][.='1']"
    );

    // Second phase sparse
    assertQ("test plain facet request",
        req("q", "*:*"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "true"
            ,"facet.sparse.mintags", "1" // Force sparse
            ,"facet.sparse.cutoff", "99999" // Force sparse
            ,"facet.sparse.termlookup", "true" // Force sparse
            ,"facet.field", "{!terms=" + UP + "" + (DOCS-1) + "," + UP + "0}" + FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
        ,pre+"/int[1][@name='" + UP + "999'][.='1']"
        ,pre+"/int[2][@name='" + UP + "0'][.='1']"
    );

    // Second phase sparse tiny
    assertQ("test plain facet request",
        req("q", "id:10010"
            , "indent", "true"
            ,"facet", "true"
            ,"facet.sparse", "true"
            ,"facet.sparse.mintags", "1" // Force sparse
            ,"facet.sparse.cutoff", "99999" // Force sparse
            ,"facet.sparse.termlookup", "true" // Force sparse
            ,"facet.sparse.stats", "true" // Force sparse
            ,"facet.field", "{!terms=" + UP + "10}" + FF
            ,"facet.mincount","1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
        ,pre+"/int[1][@name='" + UP + "10'][.='1']"
    );

  }

  @Test
  public void testRandomFaceting() throws Exception {
    try {
      Random rand = random();
      int iter = atLeast(100);
      init();
      addMoreDocs(0);

      for (int i=0; i<iter; i++) {
        doFacetTests();

        if (rand.nextInt(100) < 5) {
          init();
        }

        addMoreDocs(rand.nextInt(indexSize) + 1);

        if (rand.nextInt(100) < 50) {
          deleteSomeDocs();
        }
      }
    } finally {
      FieldCache.DEFAULT.purgeAllCaches();   // avoid FC insanity
    }
  }


  void doFacetTests() throws Exception {
    for (FldType ftype : types) {
      doFacetTests(ftype);
    }
  }


  List<String> multiValuedMethods = Arrays.asList(new String[]{"enum","fc"});
  List<String> singleValuedMethods = Arrays.asList(new String[]{"enum","fc","fcs"});


  void doFacetTests(FldType ftype) throws Exception {
    SolrQueryRequest req = req();
    try {
      Random rand = random();
      boolean validate = validateResponses;
      ModifiableSolrParams params = params("facet","true", "wt","json", "indent","true", "omitHeader","true");
      params.add("q","*:*", "rows","0");  // TODO: select subsets
      params.add("rows","0");


      SchemaField sf = req.getSchema().getField(ftype.fname);
      boolean multiValued = sf.getType().multiValuedFieldCache();

      int offset = 0;
      if (rand.nextInt(100) < 20) {
        if (rand.nextBoolean()) {
          offset = rand.nextInt(100) < 10 ? rand.nextInt(indexSize*2) : rand.nextInt(indexSize/3+1);
        }
        params.add("facet.offset", Integer.toString(offset));
      }

      int limit = 100;
      if (rand.nextInt(100) < 20) {
        if (rand.nextBoolean()) {
          limit = rand.nextInt(100) < 10 ? rand.nextInt(indexSize/2+1) : rand.nextInt(indexSize*2);
        }
        params.add("facet.limit", Integer.toString(limit));
      }

      if (rand.nextBoolean()) {
        params.add("facet.sort", rand.nextBoolean() ? "index" : "count");
      }

      if ((ftype.vals instanceof SVal) && rand.nextInt(100) < 20) {
        // validate = false;
        String prefix = ftype.createValue().toString();
        if (rand.nextInt(100) < 5) prefix =  TestUtil.randomUnicodeString(rand);
        else if (rand.nextInt(100) < 10) prefix = Character.toString((char)rand.nextInt(256));
        else if (prefix.length() > 0) prefix = prefix.substring(0, rand.nextInt(prefix.length()));
        params.add("facet.prefix", prefix);
      }

      if (rand.nextInt(100) < 10) {
        params.add("facet.mincount", Integer.toString(rand.nextInt(5)));
      }

      if (rand.nextInt(100) < 20) {
        params.add("facet.missing", "true");
      }

      // TODO: randomly add other facet params
      String key = ftype.fname;
      String facet_field = ftype.fname;
      if (random().nextBoolean()) {
        key = "alternate_key";
        facet_field = "{!key="+key+"}"+ftype.fname;
      }
      params.set("facet.field", facet_field);

      List<String> methods = multiValued ? multiValuedMethods : singleValuedMethods;
      List<String> responses = new ArrayList<>(methods.size());
      for (String method : methods) {
        // params.add("facet.field", "{!key="+method+"}" + ftype.fname);
        // TODO: allow method to be passed on local params?

        params.set("facet.method", method);

        // if (random().nextBoolean()) params.set("facet.mincount", "1");  // uncomment to test that validation fails

        String strResponse = h.query(req(params));
        // Object realResponse = ObjectBuilder.fromJSON(strResponse);
        // System.out.println(strResponse);

        responses.add(strResponse);
      }

      /**
      String strResponse = h.query(req(params));
      Object realResponse = ObjectBuilder.fromJSON(strResponse);
      **/

      if (validate) {
        for (int i=1; i<methods.size(); i++) {
          String err = JSONTestUtil.match("/", responses.get(i), responses.get(0), 0.0);
          if (err != null) {
            log.error("ERROR: mismatch facet response: " + err +
                "\n expected =" + responses.get(0) +
                "\n response = " + responses.get(i) +
                "\n request = " + params
            );
            fail(err);
          }
        }
      }


    } finally {
      req.close();
    }
  }

}



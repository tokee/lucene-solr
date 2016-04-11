package org.apache.lucene.search.grouping.term;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.AbstractDistinctValuesCollector;
import org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.AbstractGroupingTestCase;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.function.FunctionDistinctValuesCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;


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

public class TermMemCollectorTest extends AbstractGroupingTestCase {

  public void testPriorityQueue() {
    final int RUNS = 100;

    TermMemCollector.ScoreDocPQ pq = new TermMemCollector.ScoreDocPQ(5);
    TermMemCollector.FloatInt reuse = new TermMemCollector.FloatInt(1.0f, 0);
    for (int i = 0 ; i < RUNS ; i++) {
      final float score = random().nextFloat()*100;
      final int docID = random().nextInt(RUNS);
      reuse.setValues(score, docID);
      TermMemCollector.FloatInt returned = pq.insert(reuse);
      if (returned != null && score < reuse.getFloatVal()) {
        fail("Inserted [score=" + score + ", docID=" + docID + "] which pushed out [score=" + returned.getFloatVal()
            + ", docID=" + returned.getIntVal() + "]");
      }
    }
  }
}

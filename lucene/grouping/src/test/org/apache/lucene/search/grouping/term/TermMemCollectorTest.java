package org.apache.lucene.search.grouping.term;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.lucene.util.PriorityQueueLong;
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

@SuppressWarnings("ConstantConditions")
public class TermMemCollectorTest extends AbstractGroupingTestCase {

  public void testPriorityQueueLong() {
    PriorityQueueLong<Long> pql = new PriorityQueueLong<Long>(3) {
      @Override
      protected Long deserialize(long block, Long reuse) {
        return block;
      }

      @Override
      protected long serialize(Long element) {
        return element;
      }

      @Override
      public boolean lessThan(Long elementA, Long elementB) {
        return elementA < elementB;
      }
    };
    pql.insert(3L);
//    System.out.println("Inserted 3: " + Arrays.toString(pql.getElements()));
    pql.insert(2L);
//    System.out.println("Inserted 2: " + Arrays.toString(pql.getElements()));
    pql.insert(1L);
//    System.out.println("Inserted 1: " + Arrays.toString(pql.getElements()));
    pql.insert(4L);
//    System.out.println("Inserted 4: " + Arrays.toString(pql.getElements()));
    List<Long> extracted = new ArrayList<>(3);
    while (!pql.isEmpty()) {
      extracted.add(pql.pop(null));
    }
    assertEquals("The pq elemements should be ordered", Arrays.asList(new Long[]{2L, 3L, 4L}), extracted);
  }

  public void testFloatInt() {
    final int RUNS = 100;
    for (int i = 0 ; i < RUNS ; i++) {
      float f1 = random().nextFloat()*100;
      float f2 = random().nextFloat()*100;
      int doc1 = random().nextInt(100);
      int doc2 = random().nextInt(100);
      TermMemCollector.FloatInt fi1 = new TermMemCollector.FloatInt(f1, doc1);
      TermMemCollector.FloatInt fi2 = new TermMemCollector.FloatInt(f2, doc2);
      long c1 = fi1.getCompound();
      long c2 = fi2.getCompound();
      if (f1 < f2) {
        assertTrue("Expected f1(" + f1 + ") < f2 (" + f2 + ") for " + fi1 + " and " + fi2, fi1.compareTo(fi2) < 0);
        assertTrue("Expected f1(" + f1 + ") < f2 (" + f2 + ") for compound " + fi1 + " and " + fi2, c1 < c2);
      } else if (f1 > f2) {
        assertTrue("Expected f1(" + f1 + ") > f2 (" + f2 + ") for " + fi1 + " and " + fi2, fi1.compareTo(fi2) > 0);
        assertTrue("Expected f1(" + f1 + ") > f2 (" + f2 + ") for compound " + fi1 + " and " + fi2, c1 > c2);
      } else {
        assertTrue("Expected f1(" + f1 + ") == f2 (" + f2 + ") for " + fi1 + " and " + fi2, fi1.compareTo(fi2) == 0);
        assertTrue("Expected f1(" + f1 + ") == f2 (" + f2 + ") for compound " + fi1 + " and " + fi2, c1 == c2);
      }
    }
  }

  public void testPriorityQueueFixed() {
    TermMemCollector.ScoreDocPQ pq = new TermMemCollector.ScoreDocPQ(5);
    final float MISSING = 98.74208f;
    for (float f: new float[]{73.11469f, 90.14476f, 49.682255f, 98.58769f, 85.7124f}) {
      pq.insert(new TermMemCollector.FloatInt(f, 87));
    }
    pq.insert(new TermMemCollector.FloatInt(MISSING, 87));
    boolean found = false;
    for (int i = 1 ; i < pq.size() ; i++) {
      if (Math.abs(new TermMemCollector.FloatInt(pq.getElements()[i]).getFloatVal() - MISSING) < 0.0001f) {
        found = true;
        break;
      }
    }
    assertTrue("The value " + MISSING + " should be in the queue " + dump(pq), found);
  }

  public void testPriorityQueueMonkey() {
    final int RUNS = 100;
    final int SIZE = 5;
    Random random = new Random(2); // Fairly fast fail

    List<Float> control = new ArrayList<>(RUNS);

    TermMemCollector.ScoreDocPQ pq = new TermMemCollector.ScoreDocPQ(SIZE);
    TermMemCollector.FloatInt reuse = new TermMemCollector.FloatInt(1.0f, 0);
    for (int i = 0 ; i < RUNS ; i++) {
      final float score = random.nextFloat()*100;
      final int docID = random.nextInt(RUNS);
      reuse.setValues(score, docID);

      //System.out.print("Inserting " + reuse + " -> ");
      TermMemCollector.FloatInt returned = pq.insert(reuse);
      //System.out.println(returned + ":\t" + dump(pq));
      if (returned != null && score < returned.getFloatVal()) {
        fail("Inserted [score=" + score + ", docID=" + docID + "] which pushed out [score=" + returned.getFloatVal()
            + ", docID=" + returned.getIntVal() + "]");
      }
      control.add(score);
    }
    Collections.sort(control);

    control = control.subList(control.size()-SIZE, control.size());
    List<Float> actual = new ArrayList<>(SIZE);
    for (int i = 0 ; i < SIZE ; i++) {
      actual.add(pq.pop(null).getFloatVal());
    }
    assertEquals("The top-" + SIZE + " floats should be correct", control.toString(), actual.toString());
  }

  private String dump(TermMemCollector.ScoreDocPQ pq) {
    StringBuilder sb = new StringBuilder(100);
    sb.append("[");
    for (int i = 1 ; i <= pq.size() ; i++) {
      TermMemCollector.FloatInt value = new TermMemCollector.FloatInt(pq.getElements()[i]);
      if (i != 1) {
        sb.append(", ");
      }
      sb.append(String.format("%5.2f", value.getFloatVal()));
      //sb.append(String.format("(%5.2f, %d)", value.getFloatVal(), value.getIntVal()));
    }
    sb.append("]");
    return sb.toString();
  }
}

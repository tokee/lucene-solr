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
package org.apache.lucene.index;


import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.lucene70.IndexedDISICacheFactory;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/** Tests helper methods in DocValues */
public class TestDocValues extends LuceneTestCase {
  
  /** 
   * If the field doesn't exist, we return empty instances:
   * it can easily happen that a segment just doesn't have any docs with the field.
   */
  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    iw.addDocument(new Document());
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "bogus"));
    assertNotNull(DocValues.getNumeric(r, "bogus"));
    assertNotNull(DocValues.getSorted(r, "bogus"));
    assertNotNull(DocValues.getSortedSet(r, "bogus"));
    assertNotNull(DocValues.getSortedNumeric(r, "bogus"));
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field just doesnt have any docvalues at all: exception
   */
  public void testMisconfiguredField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
   
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with numeric docvalues
   */
  public void testNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }

  /**
   * Triggers varying bits per value codec representation for numeric.
   */
  public void testNumericFieldVaryingBPV() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    long generatedSum = 0;
    for (int bpv = 2 ; bpv < 24 ; bpv+=3) {
      for (int i = 0 ; i < 66000 ; i++) {
        Document doc = new Document();
        int max = 1 << (bpv - 1);
        int value =  random().nextInt(max) | max;
        generatedSum += value;
        //System.out.println("--- " + value);
        doc.add(new NumericDocValuesField("foo", value));
        iw.addDocument(doc);
      }
    }
    iw.flush();
    iw.forceMerge(1, true);
    iw.commit();
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    NumericDocValues numDV = DocValues.getNumeric(r, "foo");

    assertNotNull(numDV);
    long sum = 0;
    while (numDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      sum += numDV.longValue();
    }
    assertEquals("The sum of retrieved values should match the input", generatedSum, sum);

//    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    dr.close();
    iw.close();
    dir.close();
  }

  // TODO (Toke): Remove this when LUCENE-8374 is ready for release
  // TODO (Toke): Force the test to use the Lucene70 codec
  // IMPORTANT: This _does not yet_ trigger the varying BPS codec part, so it it measuring absolutely wrong
  public void testNumericRetrievalSpeed() throws IOException {
    final int RUNS = 2;
    final int[] QUERIES = new int[]{10, 100, 1_000, 10_000, 100_000};
    final int[] DOCS_PER_BPV = new int[]{10, 100, 1_000, 10_000, 100_000, 6_600_000};

    boolean oldDebug = IndexedDISICacheFactory.DEBUG;

    for (int docsPerBPV: DOCS_PER_BPV) {
      IndexedDISICacheFactory.DEBUG = false;
      System.out.println("Generating plain index");
      Directory dirPlain = new MMapDirectory(Paths.get(System.getProperty("java.io.tmpdir"), "plain_" + random().nextInt()));
      generateVaryingBPVIndex(dirPlain, 2, 3, 24, docsPerBPV, false);
      System.out.println("Generating optimized index");
      Directory dirOptimize = new MMapDirectory(Paths.get(System.getProperty("java.io.tmpdir"), "optimized_" + random().nextInt()));
      generateVaryingBPVIndex(dirOptimize, 2, 3, 24, docsPerBPV, true);

      IndexedDISICacheFactory.DEBUG = false;
      // Disk cache warm
      numericRetrievalSpeed(dirPlain, false, 1, 10_000, false, false);
      numericRetrievalSpeed(dirOptimize, true, 1, 10_000, true, false);
      System.out.println("Running performance tests");
      for (int run = 0; run < RUNS; run++) {
        for (int queries: QUERIES) {
          numericRetrievalSpeed(dirPlain, false, 10, queries, false, true);
          numericRetrievalSpeed(dirPlain, false, 10, queries, true, true);
          numericRetrievalSpeed(dirOptimize, true, 10, queries, false, true);
          numericRetrievalSpeed(dirOptimize, true, 10, queries, true, true);
          System.out.println("");
        }
        System.out.println("----------------------");
      }

      dirPlain.close();
      dirOptimize.close();
    }
    IndexedDISICacheFactory.DEBUG = oldDebug;
  }

  // Returns best docs/s
  private double numericRetrievalSpeed(
      Directory dir, boolean optimize, int runs, int queries, boolean lucene8374, boolean print) throws IOException {

    IndexedDISICacheFactory.BLOCK_CACHING_ENABLED = lucene8374;
    IndexedDISICacheFactory.DENSE_CACHING_ENABLED = lucene8374;
    IndexedDISICacheFactory.VARYINGBPV_CACHING_ENABLED = lucene8374;

    DirectoryReader dr = DirectoryReader.open(dir);
    int maxDoc = dr.maxDoc();
    final Set<String> fields = new HashSet<>();
    fields.add("dv");

    // Performance
    long best = Long.MAX_VALUE;
    long worst = -1;
    long sum = -1;
    for (int run = 0 ; run < runs ; run++) {
      long runTime = -System.nanoTime();
      for (int q = 0 ; q < queries ; q++) {
        final int docID = random().nextInt(maxDoc-1);

        int readerIndex = dr.readerIndex(docID);
        LeafReader reader = dr.leaves().get(readerIndex).reader();
        NumericDocValues numDV = reader.getNumericDocValues("dv");
        if (!numDV.advanceExact(docID-dr.readerBase(readerIndex))) {
          //System.err.println("Expected numeric doc value for docID=" + docID);
          continue;
        }
        sum += numDV.longValue();
      }
      runTime += System.nanoTime();
      best = Math.min(best, runTime);
      worst = Math.max(worst, runTime);
    }
    double bestDPS = queries / (best/1000000.0/1000);
    double worstDPS = queries / (worst/1000000.0/1000);
    if (print) {
      System.out.println(String.format("docs=%d, optimize=%5b, lucene8374=%5b, queries=%5s, worst/best docs/s=%5.0fK /%5.0fK",
          maxDoc, optimize, lucene8374, queries < 1000 ? queries : (queries / 1000) + "K", worstDPS / 1000, bestDPS / 1000));
    }
    assertFalse("There should be at least 1 long value", sum == -1);

    dr.close();
    return bestDPS;
  }

  private void generateVaryingBPVIndex(
      Directory dir, int bpvMin, int bpvStep, int bpvMax, int docsPerBPV, boolean optimize) throws IOException {
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    int id = 0;
    for (int bpv = bpvMin ; bpv < bpvMax+1 ; bpv += bpvStep) {
      for (int i = 0 ; i < docsPerBPV ; i++) {
        Document doc = new Document();
        int max = 1 << (bpv - 1);
        int value =  random().nextInt(max) | max;
        doc.add(new StringField("id", Integer.toString(id++), Field.Store.YES));
        if (id % 87 != 0) { // Ensure sparse
          doc.add(new NumericDocValuesField("dv", value));
        }
        iw.addDocument(doc);
      }
    }
    iw.flush();
    if (optimize) {
      iw.forceMerge(1, true);
    }
    iw.commit();
    iw.close();
  }

  /**
   * field with binary docvalues
   */
  public void testBinaryField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sorted docvalues
   */
  public void testSortedField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "foo"));
    assertNotNull(DocValues.getSorted(r, "foo"));
    assertNotNull(DocValues.getSortedSet(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sortedset docvalues
   */
  public void testSortedSetField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getSortedSet(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sortednumeric docvalues
   */
  public void testSortedNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
        DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }

  public void testAddNullNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    if (random().nextBoolean()) {
      doc.add(new NumericDocValuesField("foo", null));
    } else {
      doc.add(new BinaryDocValuesField("foo", null));
    }
    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> iw.addDocument(doc));
    assertEquals("field=\"foo\": null value not allowed", iae.getMessage());
    IOUtils.close(iw, dir);
  }
}

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
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

  // LUCENE-8374 had a bug where a vBPV-block with BPV==0 as the very end of the numeric DocValues mad it fail
  public void testNumericEntryZeroes() throws IOException {
    // Create corpus
    Path zeroPath = Paths.get(System.getProperty("java.io.tmpdir"),"plain_" + random().nextInt());
    Directory zeroDir = new MMapDirectory(zeroPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setCodec(Codec.forName("Lucene70"));
    IndexWriter iw = new IndexWriter(zeroDir, iwc);

    for (int id = 0 ; id < 2*16384 ; id++) { // 2 vBPV-blocks for the dv-field
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));

      if (id < 16384) { // First vBPV-block just has semi-ramdom values
        doc.add(new NumericDocValuesField("dv", id % 1000));
      } else {          // Second block is all zeroes, resulting in an extreme "1-byte for the while block"-representation
        doc.add(new NumericDocValuesField("dv", 0));
      }
      iw.addDocument(doc);
    }
    iw.flush();
    iw.commit();
    iw.forceMerge(1, true);
    iw.close();

    // Iterate corpus

    // Ensure that LUCENE-8374 is enabled for vBPV
    IndexedDISICacheFactory.VARYINGBPV_CACHING_ENABLED = true;
    DirectoryReader dr = DirectoryReader.open(zeroDir);
    long sum = -1;
    for (int id: new int[]{0, 1000, 16383, 16384, 16385, 2*16384-1}) {
      int readerIndex = dr.readerIndex(id);
      NumericDocValues numDV = dr.leaves().get(readerIndex).reader().getNumericDocValues("dv");
      assertTrue("There should be a value for doc " + id, numDV.advanceExact(id));
      sum += numDV.longValue() + 1;
    }
    assertNotSame("Sanity check: Sum should be > -1", -1, sum);

    // Clean up

    deleteAndClose(zeroDir);
    Files.delete(zeroPath);
  }

  // TODO (Toke): Remove this when LUCENE-8374 is ready for release
  // Note: vBPV only helps for segments with > 16384 values for the DV-field
  @Slow
  public void testNumericRetrievalSpeed() throws IOException {
    final int BPV_MIN = 2;
    final int BPV_STEP = 3;
    final int BPV_MAX = 24;

    final int MAJOR_RUNS = 1;
    final int INNER_RUNS = 10;
    //final int[] DOCS_PER_BPV = new int[]{100, 10_000, 500_000, 2_000_000};
    final int[] DOCS_PER_BPV = new int[]{5_000_000};
    //final int[] QUERIES = new int[]{10, 100, 1_000, 10_000, 100_000};
    final int[] QUERIES = new int[]{1_000_000};
    final boolean LEAVE_TEST_INDEXES = true; // Indexes are stored in TMP. Remember to delete manually if set to true

    boolean oldDebug = IndexedDISICacheFactory.DEBUG;

    for (int docsPerBPV: DOCS_PER_BPV) {
      IndexedDISICacheFactory.DEBUG = false;
      String POST_DESIGNATION =
          "bpvmin=" + BPV_MIN + "_bpvstep=" + BPV_STEP + "_bpvmax=" + BPV_MAX + "_docsperbpv=" + docsPerBPV;
      Path pathPlain = Paths.get(System.getProperty("java.io.tmpdir"),"plain_" + POST_DESIGNATION);
      boolean plainExists = Files.isDirectory(pathPlain);
      Directory dirPlain = new MMapDirectory(pathPlain);
      if (plainExists) {
        System.out.println("Skipping plain index creation as folder " + pathPlain + " already exists");
      } else {
        System.out.println("Generating plain index with " + POST_DESIGNATION);
        generateVaryingBPVIndex(dirPlain, BPV_MIN, BPV_STEP, BPV_MAX, docsPerBPV, false);
      }

      Path pathOptimize = Paths.get(System.getProperty("java.io.tmpdir"),"optimized_" + POST_DESIGNATION);
      boolean optimizedExists = Files.isDirectory(pathOptimize);
      Directory dirOptimize = new MMapDirectory(pathOptimize);
      if (optimizedExists) {
        System.out.println("Skipping optimized index creation as folder " + pathOptimize + " already exists");
      } else {
        System.out.println("Generating optimized index with " + POST_DESIGNATION);
        generateVaryingBPVIndex(dirOptimize, BPV_MIN, BPV_STEP, BPV_MAX, docsPerBPV, true);
      }

      // Disk cache warm
      final double[] NONE = new double[]{-1d, -1d};

      System.out.println("Running performance tests (note: Indexes < 16K docs are unlikely to have usable LUCENE-8374 caches)");

      for (boolean sequential : new boolean[]{false, true}) {
        System.out.println("************** " + (sequential ? "Sequential" : "Random") + " access");
        for (int run = 0; run < MAJOR_RUNS; run++) {
          System.out.println(DV_PERFORMANCE_HEADER);
          for (int queries : QUERIES) {
            for (boolean optimize : new boolean[]{false, true}) {
              Directory dir = optimize ? dirOptimize : dirPlain;
              // Warm
              numericRetrievalSpeed(dir, 5, 1000, true, true, true, false, NONE, false, true);

              // Establish baseline
              double[] basePlain = numericRetrievalSpeed(dir, INNER_RUNS, queries, false, false, false, true, NONE, sequential, true);
              numericRetrievalSpeed(dir, INNER_RUNS, queries, false, false, false, true, basePlain, sequential, true);

              numericRetrievalSpeed(dir, INNER_RUNS, queries, true, false, false, true, basePlain, sequential, true);
              numericRetrievalSpeed(dir, INNER_RUNS, queries, false, true, false, true, basePlain, sequential, true);
              numericRetrievalSpeed(dir, INNER_RUNS, queries, false, false, true, true, basePlain, sequential, true);
              numericRetrievalSpeed(dir, INNER_RUNS, queries, true, true, true, true, basePlain, sequential, true);

              // Run baseline again and compare to old to observe measuring skews due to warming and chance
              numericRetrievalSpeed(dir, INNER_RUNS, queries, false, false, false, true, basePlain, sequential, true);
              numericRetrievalSpeed(dir, INNER_RUNS, queries, false, false, false, true, basePlain, sequential, true);
              System.out.println("");
            }
          }
          if (run < MAJOR_RUNS-1) {
            System.out.println("----------------------");
          }
        }
      }

      if (LEAVE_TEST_INDEXES) {
        dirPlain.close();
        dirOptimize.close();
      } else {
        deleteAndClose(dirPlain);
        Files.delete(pathPlain);
        deleteAndClose(dirOptimize);
        Files.delete(pathOptimize);
      }
    }
    IndexedDISICacheFactory.DEBUG = oldDebug;
  }

  private void deleteAndClose(Directory dir) throws IOException {
    String[] files = dir.listAll();
    for (String file: files) {
      dir.deleteFile(file);
    }
    dir.close();
  }

  public static final String DV_PERFORMANCE_HEADER = "  docs segments requests block dense  vBPV worst_r/s best_r/s  worst/base best/base";
  public static final String DV_PERFORMANCE_PATTERN = "%6s %8s %8s %5s %5s %5s %9s %8s %10.0f%% %9.0f%%";

  // Returns [worst, best] docs/s
  private double[] numericRetrievalSpeed(
      Directory dir, int runs, int requests, boolean block, boolean dense, boolean vBPV, boolean print, double[] base,
      boolean sequential, boolean reuseDVReader) throws IOException {

    IndexedDISICacheFactory.BLOCK_CACHING_ENABLED = block;
    IndexedDISICacheFactory.DENSE_CACHING_ENABLED = dense;
    IndexedDISICacheFactory.VARYINGBPV_CACHING_ENABLED = vBPV;

    DirectoryReader dr = DirectoryReader.open(dir);
    int maxDoc = dr.maxDoc();

    long best = Long.MAX_VALUE;
    long worst = -1;
    long sum = -1;
    int sequentialDocID = -1;

    int lastDocID = 0;
    int lastReaderIndex = -1;
    NumericDocValues numDV = null;
    for (int run = 0 ; run < runs ; run++) {
      long runTime = -System.nanoTime();
      for (int q = 0 ; q < requests ; q++) {
        sequentialDocID++;
        if (sequentialDocID == maxDoc) {
          sequentialDocID = 0;
        }
        final int docID = sequential ? sequentialDocID : random().nextInt(maxDoc-1);
        int readerIndex = 0;
        if (!reuseDVReader || docID < lastDocID || (readerIndex = dr.readerIndex(docID)) != lastReaderIndex ||
            numDV == null) {
          numDV = dr.leaves().get(readerIndex).reader().getNumericDocValues("dv");
        }
        lastDocID = docID;
        lastReaderIndex = readerIndex;
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
    double worstDPS = requests / (worst/1000000.0/1000);
    double bestDPS = requests / (best/1000000.0/1000);
    double worstRelative = base[0] < 0 ? 100 : worstDPS*100/base[0];
    double bestRelative = base[1] < 0 ? 100 : bestDPS*100/base[1];
    if (print) {
      System.out.println(String.format(DV_PERFORMANCE_PATTERN,
          shorten(maxDoc), dr.leaves().size(), shorten(requests),
          block ? "block" : "", dense ? "dense" : "", vBPV ? "vBPV" : "",
          shortenKB((int) worstDPS), shortenKB((int) bestDPS), worstRelative, bestRelative));
    }
    assertFalse("There should be at least 1 long value", sum == -1);

    dr.close();
    return new double[]{worstDPS, bestDPS};
  }

  private String shortenKB(int requests) {
    return requests >= 1_000 ? requests/1_000+"K" : requests+"";
  }
  private String shorten(int requests) {
    return requests >= 1_000_000 ? requests/1_000_000+"M" : requests >= 1_000 ? requests/1_000+"K" : requests+"";
  }

  private void generateVaryingBPVIndex(
      Directory dir, int bpvMin, int bpvStep, int bpvMax, int docsPerBPV, boolean optimize) throws IOException {

    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setCodec(Codec.forName("Lucene70"));
    IndexWriter iw = new IndexWriter(dir, iwc);

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

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
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
// TODO LUCENE-8585: Attempt to force Lucene80DocValues. Remove before release
@LuceneTestCase.SuppressCodecs({"SimpleText", "Direct", "Lucene50", "Lucene60", "Lucene70", "MockRandom"})
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



  // The LUCENE-8585 jump-tables enables O(1) skipping of IndexedDISI blocks,
  // DENSE block lookup and numeric multi blocks. This test focuses on random
  // jumps
  @Slow
  public void testNumericFieldJumpTables() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    final int maxDoc = atLeast(3*65536); // Must be above 3*65536 to trigger IndexedDISI block skips
    for (int i = 0 ; i < maxDoc ; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      if (random().nextInt(100) > 10) { // Skip 10% to make DENSE blocks
        int value = random().nextInt(100000);
        doc.add(new NumericDocValuesField("dv", value));
        doc.add(new StringField("storedValue", Integer.toString(value), Field.Store.YES));
      }
      iw.addDocument(doc);
    }
    iw.flush();
    iw.forceMerge(1, true); // Single segment to force large enough structures
    iw.commit();

    try (DirectoryReader dr = DirectoryReader.open(iw)) {
        assertEquals("maxDoc should be as expected", maxDoc, dr.maxDoc());
        LeafReader r = getOnlyLeafReader(dr);

      // Check non-jumping first
      {
        NumericDocValues numDV = DocValues.getNumeric(r, "dv");
        for (int i = 0; i < maxDoc; i++) {
          IndexableField fieldValue = r.document(i).getField("storedValue");
          if (fieldValue == null) {
            assertFalse("There should be no DocValue for document #" + i, numDV.advanceExact(i));
          } else {
            assertTrue("There should be a DocValue for document #" + i, numDV.advanceExact(i));
            assertEquals("The value for document #" + i + " should be correct",
                fieldValue.stringValue(), Long.toString(numDV.longValue()));
          }
        }
      }

      {
        for (int jump = 8191; jump < maxDoc; jump += 8191) { // Smallest jump-table block (vBPV) is 16384 values
          // Create a new instance each time to ensure jumps from the beginning
          NumericDocValues numDV = DocValues.getNumeric(r, "dv");
          for (int index = 0; index < maxDoc; index += jump) {
            IndexableField fieldValue = r.document(index).getField("storedValue");
            if (fieldValue == null) {
              assertFalse("There should be no DocValue for document #" + jump, numDV.advanceExact(index));
            } else {
              assertTrue("There should be a DocValue for document #" + jump, numDV.advanceExact(index));
              assertEquals("The value for document #" + jump + " should be correct",
                  fieldValue.stringValue(), Long.toString(numDV.longValue()));
            }
          }
        }
      }
    }

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

  // LUCENE-8585 has a bug where a vBPV-block with BPV==0 as the very end of the numeric DocValues makes it fail
  //java.lang.IndexOutOfBoundsException: 24649
  //  	at __randomizedtesting.SeedInfo.seed([A1D4EA1163EBC724:FC63056775290A90]:0)
  public void testNumericEntryZeroesLastBlock() throws IOException {
    List<Long> docValues = new ArrayList<>(2*16384);
    for (int id = 0 ; id < 2*16384 ; id++) { // 2 vBPV-blocks for the dv-field
      if (id < 16384) { // First vBPV-block just has semi-ramdom values
        docValues.add((long) (id % 1000));
      } else {          // Second block is all zeroes, resulting in an extreme "1-byte for the while block"-representation
        docValues.add(0L);
      }
    }
    assertRandomAccessDV("Last block BPV=0", docValues);
  }

  private void assertRandomAccessDV(String designation, List<Long> docValues) throws IOException {
    // Create corpus
    Path zeroPath = Paths.get(System.getProperty("java.io.tmpdir"),"plain_" + random().nextInt());
    Directory zeroDir = new MMapDirectory(zeroPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    //iwc.setCodec(Codec.forName("Lucene70"));
    IndexWriter iw = new IndexWriter(zeroDir, iwc);

    for (int id = 0 ; id < docValues.size() ; id++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
      doc.add(new NumericDocValuesField("dv", docValues.get(id)));
      iw.addDocument(doc);
    }
    iw.flush();
    iw.commit();
    iw.forceMerge(1, true);
    iw.close();

    DirectoryReader dr = DirectoryReader.open(zeroDir);
    // TODO LUCENE-8585: Debug-change. Iterate from zero
    //for (int id = 0 ; id < docValues.size() ; id++) {
    for (int id = docValues.size()-2 ; id < docValues.size() ; id++) {
      int readerIndex = dr.readerIndex(id);
      // We create a new reader each time as we want to test vBPV-skipping and not sequential iteration
      NumericDocValues numDV = dr.leaves().get(readerIndex).reader().getNumericDocValues("dv");
      assertTrue(designation + ": There should be a value for docID " + id, numDV.advanceExact(id));
      assertEquals(designation + ": The value for docID " + id + " should be as expected",
          docValues.get(id), Long.valueOf(numDV.longValue()));
    }
    dr.close();

    // Clean up
    deleteAndClose(zeroDir);
    Files.delete(zeroPath);
  }

  private void deleteAndClose(Directory dir) throws IOException {
    String[] files = dir.listAll();
    for (String file: files) {
      dir.deleteFile(file);
    }
    dir.close();
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

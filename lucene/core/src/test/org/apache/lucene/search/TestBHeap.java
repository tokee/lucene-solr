package org.apache.lucene.search;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;

/**
 * This test is an explorative performance test.
 *
 * The goal is to determine if there are any gains in switching from the Object-heavy HitQueue in Solr to an
 * array-based equivalent. Candidates so far are {@link HitQueueArray} and
 * {@link HitQueuePacked}. Note that the focus for now is performance. As such, the new
 * classes has only loosely (through debugging runs) been inspected for correctness and probably contains errors.
 *
 * The test does not emulate a full search. It only allocates, randomly fills and empties the queues.
 *
 * When running the tests, there are 4 different designations:
 * - Sentinel:   The default Solr HitQueue with sentinel objects, used for standard Solr top-X searches.
 * - No_Sentinel: Same as Sentinel, but without sentinel objects.
 * - Array:  Instead of storing the heap as an array of Objects, two atomic arrays (one for scores, one for docIDs)
 *           are used.
 * - Packed: Instead of storing the heap as an array of Objects, a single atomic array of longs is used, where score
 *           and docID is packed together.
 */
public class TestBHeap extends LuceneTestCase {

  public void test1() {
    BHeap heap = new BHeap(20, 2);
    heap.insert(100);
    assertEquals("Element 1,1 should be correct", 100, heap.elements[5]);
//    heap.dump();
  }

  public void test2() {
    BHeap heap = new BHeap(20, 2);
    heap.insert(100);
    heap.insert(99);
    assertEquals("Element 1,1 should be correct\n" + heap.toString(true), 99, heap.elements[5]);
  }

  public void testFullFirstMiniheap() {
    BHeap heap = new BHeap(20, 2);
    heap.insert(100);
    heap.insert(99);
    heap.insert(101);
    assertEquals("Element 1,1 should be correct\n" + heap.toString(true), 99, heap.elements[5]);
    assertEquals("Element 1,2 should be correct\n" + heap.toString(true), 100, heap.elements[6]);
    assertEquals("Element 1,3 should be correct\n" + heap.toString(true), 101, heap.elements[7]);
  }

  public void testTwoMiniheaps() {
    BHeap heap = new BHeap(20, 2);
    heap.insert(100);
    heap.insert(99);
    heap.insert(101);
    heap.insert(87);
    assertElement(heap, 1, 1, 87);
    assertElement(heap, 1, 2, 99);
    assertElement(heap, 1, 3, 101);
    assertElement(heap, 2, 1, 100);
  }

  private void assertElement(BHeap heap, int mhIndex, int mhOffset, long expected) {
    assertEquals("Element " + mhIndex + ", " + mhOffset + " should be correct\n" + heap.toString(true),
        expected, heap.get(mhIndex, mhOffset));
  }
}

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

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

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
    assertHeap(heap, new long[][]{
        {99, 100}
    });
  }

  public void testFullFirstMiniheap() {
    BHeap heap = new BHeap(20, 2);
    insert(heap, 100, 99, 101);
    assertHeap(heap, new long[][]{
        {99, 100, 101}
    });
  }

  public void testTwoMiniheaps() {
    BHeap heap = new BHeap(20, 2);
    insert(heap, 100, 99, 101);
    insert(heap, 87);
    assertHeap(heap, new long[][]{
        {87, 99, 101},
        {100}
    });
  }

  public void testTinyHeap() {
    BHeap heap = new BHeap(3, 2);
    insert(heap, 100, 99, 101);
    assertHeap(heap, new long[][]{
        {99, 100, 101}
    });
  }

  public void testTinyHeapOverflow() {
    BHeap heap = new BHeap(3, 2);
    insert(heap, 100, 99, 101);
    insert(heap, 102);
    assertHeap(heap, new long[][]{
        {100, 102, 101}
    });
  }

  public void testTwoMiniheapsOverflow() {
    BHeap heap = new BHeap(4, 2);
    insert(heap, 100, 99, 101, 102);
    assertHeap(heap, new long[][]{
        {99, 100, 101},
        {102}
    });
    assertFlush(heap, 99,100, 101, 102);
  }

  public void testAlternateSmall() {
    BHeap heap = new BHeap(20, 2);
    insert(heap, 100, 99, 101, 102);
    assertHeap("Initial", heap, new long[][]{
        {99, 100, 101},
        {102}
    });

    heap.pop();
    assertHeap("Pop 1", heap, new long[][]{
        {100, 102, 101}
    });

    insert(heap, 87);
    assertHeap("Insert 87", heap, new long[][]{
        {87, 100, 101},
        {102}
    });

    insert(heap, 110);
    assertHeap("Insert 110", heap, new long[][]{
        {87, 100, 101},
        {102, 110}
    });

    insert(heap, 115);
    assertHeap("Insert 115", heap, new long[][]{
        {87, 100, 101},
        {102, 110, 115}
    });

    heap.pop();
    assertHeap("Pop 2", heap, new long[][]{
        {100, 102, 101},
        {115, 110}
    });

    heap.pop();
    assertHeap("Pop 3", heap, new long[][]{
        {101, 102, 110},
        {115}
    });

    heap.pop();
    assertHeap("Pop 4", heap, new long[][]{
        {102, 115, 110}
    });

    assertFlush("Final flush", heap, 102, 110, 115);
  }

  public void test1_1() {
    BHeap heap = new BHeap(1, 2);
    insert(heap, 100);
    assertHeap(heap, new long[][]{
        {100}
    });

    insert(heap, 99);
    assertHeap(heap, new long[][]{
        {100}
    });
    insert(heap, 101);
    assertHeap(heap, new long[][]{
        {101}
    });
  }

  public void testMonkeySmall() {
    testMonkeyMulti(10, 1000, 10000, 8);
  }

  // Failed at one point
  public void testMonkeySpecific() {
    testMonkey(1, 5, 19, 2, 87L);
  }
  public void testMonkeyReproduced() {
    final long[] INSERTS = new long[]{
        1559930263, 1905463594, 959321707, 1614690370, 1910156708, 1564936362
    };
    BHeap heap = new BHeap(5, 2);
    insert(heap, INSERTS);

    Arrays.sort(INSERTS);
    long[] last5 = new long[5];
    System.arraycopy(INSERTS, INSERTS.length-5, last5, 0, 5);
    System.out.println("Last 5:");
    for (long element : last5) {
      System.out.print(" " + Long.toString(element));
    }
    System.out.println();
    assertFlush(heap, last5);
  }

  private void testMonkeyMulti(int runs, int maxSize, int maxInserts, int maxExponent) {
    for  (int run = 1 ; run <= runs ; run++) {
      long seed = random().nextLong();
      Random random = new Random(seed);

      int size = random.nextInt(maxSize);             // 0 or more
      int inserts = random.nextInt(maxInserts);       // 0 or more
      int exponent = random.nextInt(maxExponent-1)+1; // 1 or more

      testMonkey(run, size, inserts, exponent, seed);
    }
  }

  private void testMonkey(int run, int size, int inserts, int exponent, long seed) {
    Random random = new Random(seed);
    BHeap actual = new BHeap(size, exponent);
    java.util.PriorityQueue<Long> expected = new java.util.PriorityQueue<>();

    for (int i = 0 ; i < inserts ; i++) {
      long element = random.nextInt(Integer.MAX_VALUE);
      actual.insert(element);
      expected.add(element);
      if (expected.size() > size) {
        expected.poll();
      }
    }
    System.out.println("");


    String extra = String.format(Locale.ENGLISH, "run=%d, size=%d, inserts=%d, exponent=%d, seed=" + seed,
        run, size, inserts, exponent);
    assertEquals("Size should match for " + extra,
        expected.size(), actual.size());
    for (int i = 1 ; i <= size ; i++) {
      assertEquals(String.format(Locale.ENGLISH, "Elements for pop %d should match for %s",
          i, extra),
          expected.poll(), new Long(actual.pop()));
    }
  }

  public static void assertFlush(BHeap heap, long... expected) {
    assertFlush("", heap, expected);
  }
  public static void assertFlush(String message, BHeap heap, long... expected) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(message + ". The popped value should match expected[" + i + "]\n" + heap.toString(true),
          expected[i], heap.pop());
    }
  }

  public static void insert(BHeap heap, long... elements) {
    for (long element: elements) {
      heap.insert(element);
    }
  }

  public static void assertHeap(BHeap heap, long[][] content) {
    assertHeap("", heap, content);
  }

  public static void assertHeap(String message, BHeap heap, long[][] content) {
    for (int miniheap = 1 ; miniheap <= content.length ; miniheap++) {
      long[] expected = content[miniheap-1];
      for (int offset = 1 ; offset <= expected.length ; offset++) {
        assertElement(message, heap, miniheap, offset, expected[offset-1]);
      }
    }
  }

  public static void assertElement(String message, BHeap heap, int mhIndex, int mhOffset, long expected) {
    assertEquals(message + ". Element " + mhIndex + ", " + mhOffset + " should be correct\n" + heap.toString(true),
        expected, heap.get(mhIndex, mhOffset));
  }
}

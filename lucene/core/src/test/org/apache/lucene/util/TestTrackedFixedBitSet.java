package org.apache.lucene.util;

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

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.search.DocIdSetIterator;

// Copy of TestFixedBitSet with extra tests specific for TrackedFixedBitSet
public class TestTrackedFixedBitSet extends BaseDocIdSetTestCase<TrackedFixedBitSet> {

  public void testFillTrackersTiny() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(1);
    bitset.set(0);
    TrackedFixedBitSet tracked = new TrackedFixedBitSet(bitset.bits, bitset.numBits);

    assertEquals("Tracker 1 should be of the correct length", 1, tracked.tracker1.length);
    assertEquals("Tracker 2 should be of the correct length", 1, tracked.tracker2.length);
    assertEquals("Tracker 1 should have the first bit set", 1L, tracked.tracker1[0]);
    assertEquals("Tracker 2 should have the first bit set", 1L, tracked.tracker2[0]);
  }

  public void testFillTrackersSmall() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(256); // 4 longs
    bitset.set(0);
    bitset.set(255);
    TrackedFixedBitSet tracked = new TrackedFixedBitSet(bitset.bits, bitset.numBits);

    assertEquals("Tracker 1 should be of the correct length", 1, tracked.tracker1.length);
    assertEquals("Tracker 2 should be of the correct length", 1, tracked.tracker2.length);

    assertEquals("Tracker 1 should have bits 0 and 3 set", 0b1001, tracked.tracker1[0]);
    assertEquals("Tracker 2 should have the first bit set",   0b1, tracked.tracker2[0]);
  }

  public void testFillTrackersMedium() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(64*64+1); // 65 longs (2 entries in tracker 1)
    bitset.set(0);
    bitset.set(255);
    bitset.set(4096); // Last bit
    TrackedFixedBitSet tracked = new TrackedFixedBitSet(bitset.bits, bitset.numBits);

    assertEquals("Tracker 1 should be of the correct length", 2, tracked.tracker1.length);
    assertEquals("Tracker 2 should be of the correct length", 1, tracked.tracker2.length);

    assertEquals("Tracker 1 should have bits 0 and 3 set in word 0", 0b1001, tracked.tracker1[0]);
    assertEquals("Tracker 1 should have bit 0 set in word 1",           0b1, tracked.tracker1[1]);
    assertEquals("Tracker 2 should have bit 0 and 1 set",              0b11, tracked.tracker2[0]);
  }

  public void testFillTrackersLarge() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(64*64*64+1); // 64*64+1 longs (2 entries in tracker 2)
    bitset.set(255);
    bitset.set(4096);
    bitset.set(64*64*64); // Last bit
    TrackedFixedBitSet tracked = new TrackedFixedBitSet(bitset.bits, bitset.numBits);

    assertEquals("Tracker 1 should be of the correct length", 65, tracked.tracker1.length);
    assertEquals("Tracker 2 should be of the correct length",  2, tracked.tracker2.length);

    assertEquals("Tracker 1 should have bit 3 set in word 0",     0b1000, tracked.tracker1[0]); // 255
    assertEquals("Tracker 1 should have bit 0 set in word 1",        0b1, tracked.tracker1[1]); // 4096
    assertEquals("Tracker 1 should have bit 0 set in word 64",       0b1, tracked.tracker1[64]);
    assertEquals("Tracker 2 should have bit 0 and 2 set in word 0", 0b11, tracked.tracker2[0]); // 255 & 4096
    assertEquals("Tracker 2 should have bit 0 set in word 1",        0b1, tracked.tracker2[1]); // 64*64*64
  }

  /**
   * Verify that {@link org.apache.lucene.util.TrackedFixedBitSet#set(int)} and
   * {@link org.apache.lucene.util.TrackedFixedBitSet#getAndSet(int)} updates the trackers correctly.
   */
  public void testTrackedGetSetMonkey() {
    final int RUNS = 50;
    final int MAX_UPDATES = 10000;
    final int MAX_SIZE = 64*64*64*10;

    for (int r = 0 ; r < RUNS ; r++) {
      getRandomTracked("Monkey run=1", MAX_SIZE, MAX_UPDATES);
    }
  }

  public void testTrackedSetRange128() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(256);
    bitset.set(1, 128);
    assertTrackers("bitmap(256).set(1, 128)", bitset);
  }

  public void testTrackedSetRange129() {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(256);
    bitset.set(1, 129);
    assertTrackers("bitmap(256).set(1, 129)", bitset);
  }

  @FunctionalInterface
  private interface TrackedDualCallback {
    public void update(TrackedFixedBitSet bitset, Integer start, Integer end);
  }

  private void testTrackedUpdates(String message, TrackedDualCallback callback) {
    testTrackedUpdates(message, 10, 10000, 64 * 64 * 64 * 10, callback);
  }
  private void testTrackedUpdates(String message, int runs, int maxSize, int maxUpdates, TrackedDualCallback callback) {
    for (int r = 0 ; r < runs ; r++) {
      TrackedFixedBitSet bitset = getRandomTracked(message + ". Monkey run=" + r, maxSize, maxUpdates);
      int range = random().nextInt(bitset.numBits);
      if (range > 0) {
        int start = random().nextInt((bitset.numBits-range-1)/2);
        callback.update(bitset, start, start + range);
        assertTrackers(message +
            ": update(" + start + ", " + (start+range) + ") run=" + r + ",  bitset=" + bitset.numBits, bitset);
      }
    }
  }

  public void testTrackedSetRangeMonkey() {
    testTrackedUpdates("set range", TrackedFixedBitSet::set);
  }

  public void testTrackedClearRangeMonkey() {
    testTrackedUpdates("clear range", TrackedFixedBitSet::clear);
  }

  public void testTrackedFlipRangeMonkey() {
    testTrackedUpdates("flip range", TrackedFixedBitSet::flip);
  }

  public void testTrackedWordIterator() {
    TrackedFixedBitSet bitset = getRandomTracked("WordIterator", 10000, 100); // 1% load
    int expectedDirtyCount = 0;
    for (long word: bitset.bits) {
      if (word != 0) {
        expectedDirtyCount++;
      }
    }
    {
      int wordIteratorCount = 0;
      TrackedFixedBitSet.WordIterator wi = new TrackedFixedBitSet.WordIterator(bitset);
      while (wi.nextWordNum() != TrackedFixedBitSet.WordIterator.NO_MORE_DOCS) {
        wordIteratorCount++;
      }
      assertEquals("The number of non-0 words should be correct", expectedDirtyCount, wordIteratorCount);
    }
    TrackedFixedBitSet.WordIterator wi = new TrackedFixedBitSet.WordIterator(bitset);
    while (wi.nextWordNum() != TrackedFixedBitSet.WordIterator.NO_MORE_DOCS) {
      assertFalse("The iterated word at wordNum " + wi.wordNum() + " should not be 0",
          wi.word() == 0);
      assertFalse("The directly accessed word at wordNum " + wi.wordNum() + " should not be 0",
          bitset.bits[wi.wordNum()] == 0);
    }

  }

  private TrackedFixedBitSet getRandomTracked(String message, int maxSize, int maxUpdates) {
    TrackedFixedBitSet bitset = new TrackedFixedBitSet(random().nextInt(maxSize -1)+1);
    final int updates = random().nextInt(maxUpdates /4); // Multiple updates per iteration
    for (int i = 0 ; i < updates ; i++) {
      bitset.set(random().nextInt(bitset.length()));
      bitset.getAndSet(random().nextInt(bitset.length()));
      bitset.clear(random().nextInt(bitset.length()));
      bitset.getAndClear(random().nextInt(bitset.length()));
    }
    assertTrackers(message + ": size=" + bitset.numBits + ", updates=" + updates, bitset);
    return bitset;
  }

  public void testTrackedCardinalityMonkey() {
    final int RUNS = 50;
    final int MAX_UPDATES = 10000;
    final int MAX_SIZE = 64*64*64*10;

    for (int r = 0 ; r < RUNS ; r++) {
      TrackedFixedBitSet tracked = getRandomTracked("Monkey run=1", MAX_SIZE, MAX_UPDATES);
      int expected = (int) BitUtil.pop_array(tracked.bits, 0, tracked.bits.length);
      assertEquals("At run=" + r + ", tracked cardinality should be correct", expected, tracked.cardinality());
    }
  }

  private void assertTrackers(String message, TrackedFixedBitSet tracked) {
    TrackedFixedBitSet recalculated = new TrackedFixedBitSet(tracked.bits, tracked.numBits);
    assertEquals(message + ". The size of tracker 1 should be as expected",
        recalculated.tracker1.length, tracked.tracker1.length);
    assertEquals(message + ". The size of tracker 2 should be as expected",
        recalculated.tracker2.length, tracked.tracker2.length);
    for (int i = 0 ; i < recalculated.tracker1.length ; i++) {
      assertEquals(message + ". tracker1[" + i + "] should be as expected",
          "0b" + Long.toBinaryString(recalculated.tracker1[i]), "0b" + Long.toBinaryString(tracked.tracker1[i]));
    }
    for (int i = 0 ; i < recalculated.tracker2.length ; i++) {
      assertEquals(message + ". tracker2[" + i + "] should be as expected",
          "0b" + Long.toBinaryString(recalculated.tracker2[i]), "0b" + Long.toBinaryString(tracked.tracker2[i]));
    }
  }

  @Override
  public TrackedFixedBitSet copyOf(BitSet bs, int length) throws IOException {
    final TrackedFixedBitSet set = new TrackedFixedBitSet(length);
    for (int doc = bs.nextSetBit(0); doc != -1; doc = bs.nextSetBit(doc + 1)) {
      set.set(doc);
    }
    return set;
  }

  void doGet(BitSet a, TrackedFixedBitSet b) {
    int max = b.length();
    for (int i=0; i<max; i++) {
      if (a.get(i) != b.get(i)) {
        fail("mismatch: BitSet=["+i+"]="+a.get(i));
      }
    }
  }

  void doNextSetBit(BitSet a, TrackedFixedBitSet b) {
    int aa=-1,bb=-1;
    do {
      aa = a.nextSetBit(aa+1);
      bb = bb < b.length()-1 ? b.nextSetBit(bb+1) : -1;
      assertEquals(aa,bb);
    } while (aa>=0);
  }

  void doPrevSetBit(BitSet a, TrackedFixedBitSet b) {
    int aa = a.size() + random().nextInt(100);
    int bb = aa;
    do {
      // aa = a.prevSetBit(aa-1);
      aa--;
      while ((aa >= 0) && (! a.get(aa))) {
        aa--;
      }
      if (b.length() == 0) {
        bb = -1;
      } else if (bb > b.length()-1) {
        bb = b.prevSetBit(b.length()-1);
      } else if (bb < 1) {
        bb = -1;
      } else {
        bb = bb >= 1 ? b.prevSetBit(bb-1) : -1;
      }
      assertEquals(aa,bb);
    } while (aa>=0);
  }

  // test interleaving different TrackedFixedBitSetIterator.next()/skipTo()
  void doIterate(BitSet a, TrackedFixedBitSet b, int mode) throws IOException {
    if (mode==1) doIterate1(a, b);
    if (mode==2) doIterate2(a, b);
  }

  void doIterate1(BitSet a, TrackedFixedBitSet b) throws IOException {
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = b.iterator();
    do {
      aa = a.nextSetBit(aa+1);
      bb = (bb < b.length() && random().nextBoolean()) ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doIterate2(BitSet a, TrackedFixedBitSet b) throws IOException {
    int aa=-1,bb=-1;
    DocIdSetIterator iterator = b.iterator();
    do {
      aa = a.nextSetBit(aa+1);
      bb = random().nextBoolean() ? iterator.nextDoc() : iterator.advance(bb + 1);
      assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
    } while (aa>=0);
  }

  void doRandomSets(int maxSize, int iter, int mode) throws IOException {
    BitSet a0=null;
    TrackedFixedBitSet b0=null;

    for (int i=0; i<iter; i++) {
      int sz = TestUtil.nextInt(random(), 2, maxSize);
      BitSet a = new BitSet(sz);
      TrackedFixedBitSet b = new TrackedFixedBitSet(sz);

      // test the various ways of setting bits
      if (sz>0) {
        int nOper = random().nextInt(sz);
        for (int j=0; j<nOper; j++) {
          int idx;         

          idx = random().nextInt(sz);
          a.set(idx);
          b.set(idx);
          
          idx = random().nextInt(sz);
          a.clear(idx);
          b.clear(idx);
          
          idx = random().nextInt(sz);
          a.flip(idx);
          b.flip(idx, idx+1);

          idx = random().nextInt(sz);
          a.flip(idx);
          b.flip(idx, idx+1);

          boolean val2 = b.get(idx);
          boolean val = b.getAndSet(idx);
          assertTrue(val2 == val);
          assertTrue(b.get(idx));
          
          if (!val) b.clear(idx);
          assertTrue(b.get(idx) == val);
        }
      }

      // test that the various ways of accessing the bits are equivalent
      doGet(a,b);

      // test ranges, including possible extension
      int fromIndex, toIndex;
      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      BitSet aa = (BitSet)a.clone(); aa.flip(fromIndex,toIndex);
      TrackedFixedBitSet bb = b.clone(); bb.flip(fromIndex,toIndex);

      doIterate(aa,bb, mode);   // a problem here is from flip or doIterate

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (BitSet)a.clone(); aa.clear(fromIndex,toIndex);
      bb = b.clone(); bb.clear(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from clear() or nextSetBit
      
      doPrevSetBit(aa,bb);

      fromIndex = random().nextInt(sz/2);
      toIndex = fromIndex + random().nextInt(sz - fromIndex);
      aa = (BitSet)a.clone(); aa.set(fromIndex,toIndex);
      bb = b.clone(); bb.set(fromIndex,toIndex);

      doNextSetBit(aa,bb); // a problem here is from set() or nextSetBit
    
      doPrevSetBit(aa,bb);

      if (b0 != null && b0.length() <= b.length()) {
        assertEquals(a.cardinality(), b.cardinality());

        BitSet a_and = (BitSet)a.clone(); a_and.and(a0);
        BitSet a_or = (BitSet)a.clone(); a_or.or(a0);
        BitSet a_xor = (BitSet)a.clone(); a_xor.xor(a0);
        BitSet a_andn = (BitSet)a.clone(); a_andn.andNot(a0);

        TrackedFixedBitSet b_and = b.clone(); assertEquals(b,b_and); b_and.and(b0);
        TrackedFixedBitSet b_or = b.clone(); b_or.or(b0);
        TrackedFixedBitSet b_xor = b.clone(); b_xor.xor(b0);
        TrackedFixedBitSet b_andn = b.clone(); b_andn.andNot(b0);

        assertEquals(a0.cardinality(), b0.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());

        doIterate(a_and,b_and, mode);
        doIterate(a_or,b_or, mode);
        doIterate(a_andn,b_andn, mode);
        doIterate(a_xor,b_xor, mode);
        
        assertEquals(a_and.cardinality(), b_and.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());
        assertEquals(a_xor.cardinality(), b_xor.cardinality());
        assertEquals(a_andn.cardinality(), b_andn.cardinality());
      }

      a0=a;
      b0=b;
    }
  }
  
  // large enough to flush obvious bugs, small enough to run in <.5 sec as part of a
  // larger testsuite.
  public void testSmall() throws IOException {
    doRandomSets(atLeast(1200), atLeast(1000), 1);
    doRandomSets(atLeast(1200), atLeast(1000), 2);
  }

  // uncomment to run a bigger test (~2 minutes).
  /*
  public void testBig() {
    doRandomSets(2000,200000, 1);
    doRandomSets(2000,200000, 2);
  }
  */

  public void testEquals() {
    // This test can't handle numBits==0:
    final int numBits = random().nextInt(2000) + 1;
    TrackedFixedBitSet b1 = new TrackedFixedBitSet(numBits);
    TrackedFixedBitSet b2 = new TrackedFixedBitSet(numBits);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int idx = random().nextInt(numBits);
      if (!b1.get(idx)) {
        b1.set(idx);
        assertFalse(b1.equals(b2));
        assertFalse(b2.equals(b1));
        b2.set(idx);
        assertTrue(b1.equals(b2));
        assertTrue(b2.equals(b1));
      }
    }

    // try different type of object
    assertFalse(b1.equals(new Object()));
  }
  
  public void testHashCodeEquals() {
    // This test can't handle numBits==0:
    final int numBits = random().nextInt(2000) + 1;
    TrackedFixedBitSet b1 = new TrackedFixedBitSet(numBits);
    TrackedFixedBitSet b2 = new TrackedFixedBitSet(numBits);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int idx = random().nextInt(numBits);
      if (!b1.get(idx)) {
        b1.set(idx);
        assertFalse(b1.equals(b2));
        assertFalse(b1.hashCode() == b2.hashCode());
        b2.set(idx);
        assertEquals(b1, b2);
        assertEquals(b1.hashCode(), b2.hashCode());
      }
    }
  } 

  public void testSmallBitSets() {
    // Make sure size 0-10 bit sets are OK:
    for(int numBits=0;numBits<10;numBits++) {
      TrackedFixedBitSet b1 = new TrackedFixedBitSet(numBits);
      TrackedFixedBitSet b2 = new TrackedFixedBitSet(numBits);
      assertTrue(b1.equals(b2));
      assertEquals(b1.hashCode(), b2.hashCode());
      assertEquals(0, b1.cardinality());
      if (numBits > 0) {
        b1.set(0, numBits);
        assertEquals(numBits, b1.cardinality());
        b1.flip(0, numBits);
        assertEquals(0, b1.cardinality());
      }
    }
  }
  
  private TrackedFixedBitSet makeTrackedFixedBitSet(int[] a, int numBits) {
    TrackedFixedBitSet bs;
    if (random().nextBoolean()) {
      int bits2words = TrackedFixedBitSet.bits2words(numBits);
      long[] words = new long[bits2words + random().nextInt(100)];
      for (int i = bits2words; i < words.length; i++) {
        words[i] = random().nextLong();
      }
      bs = new TrackedFixedBitSet(words, numBits);

    } else {
      bs = new TrackedFixedBitSet(numBits);
    }
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private BitSet makeBitSet(int[] a) {
    BitSet bs = new BitSet();
    for (int e: a) {
      bs.set(e);
    }
    return bs;
  }

  private void checkPrevSetBitArray(int [] a, int numBits) {
    TrackedFixedBitSet obs = makeTrackedFixedBitSet(a, numBits);
    BitSet bs = makeBitSet(a);
    doPrevSetBit(bs, obs);
  }

  public void testPrevSetBit() {
    checkPrevSetBitArray(new int[] {}, 0);
    checkPrevSetBitArray(new int[] {0}, 1);
    checkPrevSetBitArray(new int[] {0,2}, 3);
  }
  
  
  private void checkNextSetBitArray(int [] a, int numBits) {
    TrackedFixedBitSet obs = makeTrackedFixedBitSet(a, numBits);
    BitSet bs = makeBitSet(a);
    doNextSetBit(bs, obs);
  }
  
  public void testNextBitSet() {
    int[] setBits = new int[0+random().nextInt(1000)];
    for (int i = 0; i < setBits.length; i++) {
      setBits[i] = random().nextInt(setBits.length);
    }
    checkNextSetBitArray(setBits, setBits.length + random().nextInt(10));
    
    checkNextSetBitArray(new int[0], setBits.length + random().nextInt(10));
  }
  
  public void testEnsureCapacity() {
    TrackedFixedBitSet bits = new TrackedFixedBitSet(5);
    bits.set(1);
    bits.set(4);
    
    TrackedFixedBitSet newBits = TrackedFixedBitSet.ensureCapacity(bits, 8); // grow within the word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we align to 64-bits, so even though it shouldn't have, it re-allocated a long[1]
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));

    newBits.set(1);
    newBits = TrackedFixedBitSet.ensureCapacity(newBits, newBits.length() - 2); // reuse
    assertTrue(newBits.get(1));

    bits.set(1);
    newBits = TrackedFixedBitSet.ensureCapacity(bits, 72); // grow beyond one word
    assertTrue(newBits.get(1));
    assertTrue(newBits.get(4));
    newBits.clear(1);
    // we grew the long[], so it's not shared
    assertTrue(bits.get(1));
    assertFalse(newBits.get(1));
  }
  
}

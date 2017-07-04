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

package org.apache.solr.search.sparse.counters.plane;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.packed.BPVProvider;
import org.apache.solr.search.sparse.counters.Incrementable;
import org.apache.solr.search.sparse.packed.IntMutable;
import org.apache.solr.search.sparse.packed.LongTailPerformance;
import org.apache.solr.search.sparse.counters.PackedOpportunistic;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slow
public class TestNPlaneMutable extends LuceneTestCase {
  private final static int M = 1048576;
  private final static int MI = 1000000;

  // Once triggered an ArrayIndexOutOfBoundsException, now just runs random updates
  public void testMonkey() {
    final int DIVISOR = 500;
    final int UPDATES = MI;
    final int[] CACHES = new int[] {200};
    final int[] MAX_PLANES = new int[] {4};
    final int[] SPLITS = new int[] {1};
    LongTailPerformance.measurePerformance(LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR),
        9, 9/2, 2, UPDATES, 10, CACHES, MAX_PLANES, Integer.MAX_VALUE, SPLITS, true, null);
  }

  public void testMonkeyZ() {
    final int DIVISOR = 500;
    final int UPDATES = MI;
    final int[] CACHES = new int[] {200};
    final int[] MAX_PLANES = new int[] {8};
    final int[] SPLITS = new int[] {1};
    final char[] WHITELIST = null;
    LongTailPerformance.measurePerformance(LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR),
        9, 9/2, 1, UPDATES, 10, CACHES, MAX_PLANES, 1, SPLITS, true, WHITELIST);
  }

  public void testMonkeySplits() {
    final int DIVISOR = 500;
    final int UPDATES = MI;
    final int[] CACHES = new int[] {200};
    final int[] MAX_PLANES = new int[] {4, 4};
    final int[] SPLITS = new int[] {1, 2, 3, 4};
    final char[] WHITELIST = null;
    LongTailPerformance.measurePerformance(LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR),
        9, 9/2, 1, UPDATES, 10, CACHES, MAX_PLANES, 1, SPLITS, true, WHITELIST);
  }

  // TODO: Add test for creating new empty clones

  public void testMonkeySplitsWhitelist() {
    final int DIVISOR = 500;
    final int UPDATES = MI;
    final int[] CACHES = new int[] {200};
    final int[] MAX_PLANES = new int[] {4, 4};
    final int[] SPLITS = new int[] {1, 2, 3, 4};
    final char[] WHITELIST = new char[] {'f', 'g', 'h', 'i', 'l', 'm', 'n', 'o'};
    LongTailPerformance.measurePerformance(LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR),
        9, 9/2, 1, UPDATES, 10, CACHES, MAX_PLANES, 1, SPLITS, true, WHITELIST);
  }

  public void testThreadedStressOpportunistic() {
    final int SIZE = 100;
    final int UPDATES = 10*1000000;
    final int THREADS = 8;
    testThreadedStress(SIZE, UPDATES, THREADS, PackedOpportunistic.create(SIZE, 32));
  }

  public void testThreadedStress(int size, int updates, int threads, PackedInts.Mutable counters) {

    int seed = random().nextInt();
    System.out.println("testStressOpportunistic size=" + size + ", updates=" + updates/1000000
        + "M, threads=" + threads + ", seed=" + seed);
    Random random = new Random(seed);
    PackedInts.Mutable expected = new IntMutable(size);
    Incrementable expInc = new Incrementable.IncrementableMutable(expected);
    Incrementable couInc = counters instanceof Incrementable ?
        (Incrementable)counters : new Incrementable.IncrementableMutable(counters);

    // Hammer with updates
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    for (int i = 0 ; i < threads ; i++) {
      executor.submit(new Updater(expInc, couInc, size, updates, new Random(random.nextInt())));
    }
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unorderly termination while waiting for " + threads + " update threads", e);
    }

    // Correct result?
    for (int i = 0 ; i < size ;i++) {
      assertEquals("The value at index " + i + " should be correct", expected.get(i), counters.get(i));
    }

  }
  private static class Updater implements Callable<Updater> {
    private final Incrementable expected;
    private final Incrementable actual;
    private final int size;
    private final int updates;
    private final Random random;

    private Updater(Incrementable expected, Incrementable actual, int size, int updates, Random random) {
      this.expected = expected;
      this.actual = actual;
      this.size = size;
      this.updates = updates;
      this.random = random;
    }

    @Override
    public Updater call() throws Exception {
      for (int i = 0 ; i < updates ; i++) {
        final int index = random.nextInt(size);
        synchronized (expected) {
          expected.increment(index);
        }
        actual.increment(index);
      }
      return this;
    }
  }

  // Triggers problem with shift implementation
  public void testShiftWrongValues() {
    final double DIVISOR = 1/0.0001;
    final int UPDATES = MI;
    final int[] CACHES = new int[] {100, 20};
    final int[] MAX_PLANES = new int[] {4, 64};
    final int[] SPLITS = new int[] {1};

    LongTailPerformance.measurePerformance(LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR),
        9, 9/2, 1, UPDATES, 10, CACHES, MAX_PLANES, Integer.MAX_VALUE, SPLITS, true, null);
  }

  public void testOverflow() {
    final int DIVISOR = 500;
    final int CACHE = 1000;
    final int MAX_PLANES = 4;
    final int[] INCREMENTS = new int[]{999, 999};//12345, 1, 12345, 7, 1024, 999, 1000, 999, 1000};
    long[] histogram = LongTailPerformance.reduce(LongTailPerformance.links20150209, DIVISOR);

    final PackedInts.Reader maxima = LongTailPerformance.getMaxima(histogram);
    NPlaneMutable nplane =
        new NPlaneMutable(new BPVProvider.BPVPackedWrapper(maxima, false), CACHE, MAX_PLANES,
            NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, NPlaneMutable.IMPL.shift);

    checkOverflow("Before increment", nplane);
    for (int i = 0; i < INCREMENTS.length; i++) {
      int inc = INCREMENTS[i];
      while (maxima.get(inc) <= 1) {
        inc++;
        if (inc > maxima.size()) {
          inc = 0;
        }
      }
      nplane.increment(inc);
      checkOverflow("After inc(" + inc + ") #" + (i+1), nplane);
    }
  }

  private void checkOverflow(String message, NPlaneMutable nplane) {
    for (int planeIndex = 0 ; planeIndex < nplane.planes.length ; planeIndex++) {
      Plane plane = nplane.planes[planeIndex];
      if (!plane.hasOverflow) {
        continue;
      }
/*      for (int o = 0 ; o < 80 ; o++) {
        System.out.print(plane.isOverflow(o) ? "*" : ".");
      }
      System.out.println(" Plane " + planeIndex);
      for (int o = 0 ; o < 80 ; o++) {
        System.out.print((char)(plane.overflowRank(o) + 'a'));
      }
      System.out.println();*/
      int overflows = 0;
      for (int i = 0 ; i < plane.valueCount ; i++) {
        if (plane.isOverflow(i)) {
          overflows++;
          assertEquals(message + ". Rank should return #overflows-1 when the current overflow bit is set @ index " + i,
              overflows-1, plane.overflowRank(i));
        } else if (overflows > 0) {
          assertEquals(message +". Rank should return #overflows when the current overflow bit is not set @ index " + i,
              overflows, plane.overflowRank(i));
        }
      }
      if (planeIndex != 0) {
        assertEquals(message + ". The number of set overflows for plane " + planeIndex + " should match plane "
            + planeIndex+1,
            nplane.planes[planeIndex+1].valueCount, overflows);
      }
    }
  }

  public void testSmallAddSpank() {
    testSmallAdd(NPlaneMutable.IMPL.spank);
  }

  public void testSmallAddZethra() {
    testSmallAdd(NPlaneMutable.IMPL.zethra);
  }

  private void testSmallAdd(NPlaneMutable.IMPL impl) {
    final int[] MAXIMA = new int[]{10, 1, 16, 2, 3};
    final int MAX = 16;
    final PackedInts.Mutable maxima =
        PackedInts.getMutable(MAXIMA.length, PackedInts.bitsRequired(MAX), PackedInts.COMPACT);
    for (int i = 0 ; i < MAXIMA.length ; i++) {
      maxima.set(i, MAXIMA[i]);
    }
    System.out.println("maxima: " + toString(maxima));

    NPlaneMutable bpm = new NPlaneMutable(maxima, impl);
    bpm.set(1, bpm.get(1)+1);
    assertEquals("Test 1: index 1", 1, bpm.get(1));
    assertEquals("The unmodified counter 0 should be zero", 0, bpm.get(0));
//    System.out.println("=================== inc(1)\n" + bpm.toString(true));
    bpm.set(0, bpm.get(0) + 1);
//    System.out.println("=================== inc(0)\n" + bpm.toString(true));
    assertEquals("Test 2: index 0", 1, bpm.get(0));
    bpm.set(0, bpm.get(0)+1);
    bpm.set(0, bpm.get(0)+1);
    assertEquals("Test 3: index 0", 3, bpm.get(0));
    bpm.set(0, bpm.get(0)+1);
    assertEquals("Test 4: index 0", 4, bpm.get(0));
    bpm.set(2, bpm.get(2)+1);
  }

  public void testSpecificFillPattern() {
    final int[] MAXIMA = new int[]{8, 4, 12, 10, 8, 10, 15, 12, 7, 14, 0};
    final int[] INCREMENTS = new int[]{5, 6, 0, 8, 9, 8, 9, 6, 9, 2, 8, 7, 6, 5, 6, 2, 7, 1, 6, 4, 3};
    final int MAX = 15;
    final PackedInts.Mutable maxima =
        PackedInts.getMutable(MAXIMA.length, PackedInts.bitsRequired(MAX), PackedInts.COMPACT);
    for (int i = 0 ; i < MAXIMA.length ; i++) {
      maxima.set(i, MAXIMA[i]);
    }
//    System.out.println("maxima: " + toString(maxima));
    BPVProvider.StatCollectingBPVWrapper collector = new BPVProvider.StatCollectingBPVWrapper(
        new BPVProvider.BPVPackedWrapper(maxima, false));
    collector.collect();
//    System.out.println("maxima zeroBits: " + toString(collector.plusOneHistogram));

    NPlaneLayout layout = NPlaneLayout.getLayout(collector.plusOneHistogram, true);
    NPlaneMutable mutable = new NPlaneMutable(layout, new BPVProvider.BPVPackedWrapper(maxima, false),
        NPlaneMutable.IMPL.zethra);
//    System.out.println(mutable.toString(true) + "\n");
    //mutable = new NPlaneMutable(maxima, NPlaneMutable.IMPL.spank);

    for (int inc: INCREMENTS) {
      mutable.increment(inc);
    }
  }

  public void testCreateZeroTrackedFromHistogram() {
    final int[] MAXIMA = new int[]{10, 1, 16, 2, 3};
    final int MAX = 16;
    final PackedInts.Mutable maxima =
        PackedInts.getMutable(MAXIMA.length, PackedInts.bitsRequired(MAX), PackedInts.COMPACT);
    for (int i = 0 ; i < MAXIMA.length ; i++) {
      maxima.set(i, MAXIMA[i]);
    }
//    System.out.println("maxima: " + toString(maxima));
    BPVProvider.StatCollectingBPVWrapper collector = new BPVProvider.StatCollectingBPVWrapper(
        new BPVProvider.BPVPackedWrapper(maxima, false));
    collector.collect();
//    System.out.println("maxima zeroBits: " + toString(collector.plusOneHistogram));

    NPlaneLayout layout = NPlaneLayout.getLayout(collector.plusOneHistogram, true);
    NPlaneMutable mutable = new NPlaneMutable(layout, new BPVProvider.BPVPackedWrapper(maxima, false),
        NPlaneMutable.IMPL.zethra);
//    System.out.println(mutable.toString(true) + "\n");
    //mutable = new NPlaneMutable(maxima, NPlaneMutable.IMPL.spank);

    for (int i = 0 ; i < MAXIMA.length ; i++) {
      for (int j = 0 ; j < MAXIMA[i]; j++) {
        mutable.increment(i);
//        System.out.println("index=" + i + ", increment=" + (i+1) + "/" + MAXIMA[i] + ": "
//            + mutable.get(i) + "\n" + mutable.toString(true));
      }
    }
    // TODO: Overflow for plane 2 is wrong: Index 0 should be marked
    for (int i = 0 ; i < MAXIMA.length ; i++) {
        assertEquals("index=" + i, MAXIMA[i], mutable.get(i));
    }
  }

  public void testNPlaneLayout() {
    long EXPECTED = 400;
    long[] histogram = LongTailPerformance.reduce(LongTailPerformance.links20150209, 1.0);

    long[] full = NPlaneMutable.directHistogramToFullZero(histogram);
    NPlaneLayout layout = NPlaneLayout.getLayoutWithZeroHistogram(
        full, 0, 64, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, false);
    assertTrue("There should be more than 3 planes", layout.size() > 3);
    long mem = 0;
    for (Plane.PseudoPlane plane: layout) {
      mem += plane.estimateBytesNeeded(false, NPlaneMutable.IMPL.spank);
    }
    assertTrue("The size should be less than " + EXPECTED + "MB, but was " + mem / M + "MB",
        EXPECTED * M > mem);

    final long estimated = NPlaneMutable.estimateBytesNeeded(
        histogram, 0, 64, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, false, NPlaneMutable.IMPL.spank);
    assertTrue("The estimated size should be less than " + EXPECTED + "MB, but was " + estimated / M + "MB",
        EXPECTED * M > estimated);
//    System.out.println(String.format("%d planes, PseudoPlane mem %dMB, estimated mem %dMB",
//        layout.size(), mem/M, estimated/M));
  }

  public void testSmallIncSpank() {
    testSmallInc(NPlaneMutable.IMPL.spank);
  }
  public void testSmallIncZethra() {
    testSmallInc(NPlaneMutable.IMPL.zethra);
  }

  public void testSmallInc(NPlaneMutable.IMPL impl) {
    final PackedInts.Mutable maxima = toMutable(10, 1, 16, 2, 3);
    System.out.println("maxima: " + toString(maxima));
    NPlaneMutable bpm = new NPlaneMutable(maxima, impl);
    System.out.println(bpm.toString(true));

    bpm.increment(1);
    assertEquals("Test 1: index 1", 1, bpm.get(1));
    assertEquals("The unmodified counter 0 should be zero", 0, bpm.get(0));
    bpm.increment(0);
    assertEquals("Test 2: index 0", 1, bpm.get(0));
    bpm.increment(0);
    bpm.increment(0);
    assertEquals("Test 3: index 0", 3, bpm.get(0));
    bpm.increment(0);
    assertEquals("Test 4: index 0", 4, bpm.get(0));
    bpm.increment(2);
//    System.out.println("============ expected 2:" + 1);
//    System.out.println(bpm.toString(true));
    assertEquals("Test 5: index 2", 1, bpm.get(2));
    for (int i = 0 ; i < 15 ; i++) {
      bpm.increment(2);
//      System.out.println("============ expected 2:" + (1 + 1 + i));
//      System.out.println(bpm.toString(true));
      assertEquals("Test 5b: index 2", 1 + 1 + i, bpm.get(2));
    }
    assertEquals("Test 6: index 2", 16, bpm.get(2));
    bpm.increment(4);
    assertEquals("Test 7: index 4", 1, bpm.get(4));
    assertEquals("Test 1b: index 1", 1, bpm.get(1));
  }

  public void testOverflowCache() {
    final PackedInts.Mutable maxima = toMutable(10, 1, 16, 2, 3, 2, 3, 100, 140);
    NPlaneMutable bpm = new NPlaneMutable(maxima, NPlaneMutable.IMPL.spank, 5);
    final int[][] TESTS = new int[][]{
        {8, 14},
        {7, 50},
        {4, 3},
        {2, 7},
        {5, 1}
    };
    for (int[] test: TESTS) {
      assertValue(bpm, test[0], 0);
      bpm.set(test[0], test[1]);
      assertValue(bpm, test[0], test[1]);
//      System.out.println(bpm.toString(true));
    }
    for (int i = 0 ; i < maxima.size() ; i++) {
      bpm.set(i, maxima.get(i));
      assertValue(bpm, i, maxima.get(i));
    }
  }

  private void assertValue(PackedInts.Mutable maxima, int index, long expected) {
    assertEquals("The value at position " + index + " should be correct", expected, maxima.get(index));
  }

  public static PackedInts.Mutable toMutable(int... maxValues) {
    int maxMax = 0;
    for (int maxValue: maxValues) {
      if (maxValue > maxMax) {
        maxMax = maxValue;
      }
    }
    final PackedInts.Mutable maxima =
        PackedInts.getMutable(maxValues.length, PackedInts.bitsRequired(maxMax), PackedInts.COMPACT);
    for (int i = 0 ; i < maxValues.length ; i++) {
      maxima.set(i, maxValues[i]);
    }
    return maxima;
  }

  public void testRandom() {
    final int COUNTERS = 100;
    final int MAX = 1000;
    final int updates = M/100;
    final PackedInts.Reader maxima = getMaxima(COUNTERS, MAX);

    assertMonkey(maxima, updates);
  }

  public void testRandomSmallLongTail() {
//    PackedInts.Reader maxima = getMaxima(TestDualPlaneMutable.getLinksHistogram());
    PackedInts.Reader maxima = LongTailPerformance.getMaxima(LongTailPerformance.pad(1, 3, 2)); // 1 + 3*3 + 2*7 = 24
    assertMonkey(maxima, (int) sum(maxima));
  }

  private long sum(PackedInts.Reader values) {
    long total = 0;
    for (int i = 0 ; i < values.size() ; i++) {
      total += values.get(i);
    }
    return total;
  }

  public void testRandomRealWorldHistogramLongTail() {
    assertMonkey(LongTailPerformance.getMaxima(LongTailPerformance.reduce(LongTailPerformance.links20150209, 10)), M);
  }

  public void testBytesEstimation() {
    System.out.println(String.format(Locale.ENGLISH, "ltbpm=%d/%d/%dMB",
        NPlaneMutable.estimateBytesNeeded(LongTailPerformance.links20150209, NPlaneMutable.IMPL.spank) / M,
        640280533L * (NPlaneMutable.getMaxBit(LongTailPerformance.links20150209) + 1) / 8 / M,
        640280533L * 4 / M));
  }

  public void disabledtestAssignRealLargeSample() {
    PackedInts.Reader maxima = LongTailPerformance.getMaxima(LongTailPerformance.links20150209);
    NPlaneMutable bpm = new NPlaneMutable(maxima, NPlaneMutable.IMPL.spank);
    for (int i = 0 ; i < maxima.size() ; i++) {
      bpm.set(i, maxima.get(i));
      assertEquals("The set max value at index " + i + " should be correct", maxima.get(i), bpm.get(i));
    }
    for (int i = 0 ; i < maxima.size() ; i++) {
      assertEquals("The previously set value at index " + i + " should be correct", maxima.get(i), bpm.get(i));
    }
    for (int i = 0 ; i < maxima.size() ; i++) {
      bpm.set(i, maxima.get(i)-1);
      assertEquals("The set max-1 value at index " + i + " should be correct", maxima.get(i)-1, bpm.get(i));
    }
    for (int i = 0 ; i < maxima.size() ; i++) {
      bpm.increment(i);
      assertEquals("The set max-1 value + inc at index " + i + " should be correct", maxima.get(i), bpm.get(i));
    }
  }

  private void assertMonkey(PackedInts.Reader maxima, int updates) {
    NPlaneMutable bpm = new NPlaneMutable(maxima, NPlaneMutable.IMPL.spank);
    PackedInts.Mutable expected = PackedInts.getMutable(bpm.size(), bpm.getBitsPerValue(), PackedInts.FASTEST);
    System.out.println(String.format(Locale.ENGLISH, "Memory used: %d/%dMB (%4.2f%%)",
        bpm.ramBytesUsed()/M, maxima.ramBytesUsed()/M, bpm.ramBytesUsed() * 100.0 / maxima.ramBytesUsed()));
    for (int update = 0 ; update < updates ; update++) {
      int index = random().nextInt(maxima.size());
      int oldIndex = -1;
      while (expected.get(index) >= maxima.get(index)) {
        if (oldIndex == -1) {
          oldIndex = index;
        }
        index++;
        if (index == maxima.size()) {
          index = 0;
        }
        if (oldIndex == index) {
            fail("Unable to generate sample as the number of updates (" + updates + ") is higher than the " +
                "collective counts");
        }
      }
      expected.set(index, expected.get(index)+1);
      try {
        bpm.increment(index);
//        bpm.set(index, bpm.get(index));
      } catch (Exception e) {
        fail("Unexpected exception calling bmp.inc(" + index + "): " +  e.getMessage());
      }
      try {
        assertEquals("After " + (update+1) + " updates the BPM-value should be as expected",
            expected.get(index), bpm.get(index));
      } catch (Exception e) {
        fail("Unexpected exception calling bmp.get(" + index + "): " + e.getMessage());
      }
    }
  }

  private String toString(PackedInts.Reader maxima) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0 ; i < maxima.size() ; i++) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(maxima.get(i)));
    }
    return sb.toString();
  }

  private String toString(long[] bpvs) {
    StringBuilder sb = new StringBuilder();
    for (long bpv : bpvs) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(bpv));
    }
    return sb.toString();
  }

  private PackedInts.Reader getMaxima(int counters, int max) {
    final PackedInts.Mutable maxima = PackedInts.getMutable(counters, 30, PackedInts.FASTEST);
    for (int i = 0 ; i < counters ; i++) {
      maxima.set(i, random().nextInt(max-1)+1);
    }
    return maxima;
  }

  public static PackedInts.Reader oldGetMaxima(long[] histogram) {
    System.out.println("Creating random maxima from histogram...");
    long valueCount = 0;
    long maxValueBits = 0;
    long valueBits = 0;
    for (long h: histogram) {
      valueBits++;
      if (h != 0) {
        valueCount += h;
        maxValueBits = valueBits;
      }
    }
    PackedInts.Mutable maxima =
        PackedInts.getMutable((int) valueCount, (int) maxValueBits, PackedInts.FASTEST);
    int maxpos = 0;
    for (int valueBit = 1 ; valueBit <= maxValueBits; valueBit++) {
      long val = (long) Math.pow(2, valueBit)-1;
      for (int i = 0 ; i < histogram[valueBit-1] ; i++) {
        maxima.set(maxpos, val);
        maxpos++;
      }
    }
    System.out.println("Shuffling maxima...");
    shuffle(maxima);
    System.out.println("Finished maxima creation");
    return maxima;
  }

  // Fisher–Yates shuffle
  private static void shuffle(PackedInts.Mutable maxima) {
    for (int i = maxima.size()-1 ; i > 0 ; i--) {
      final int index = random().nextInt(i+1);
      long val = maxima.get(index);
      maxima.set(index, maxima.get(i));
      maxima.set(i, val);
    }
  }
}

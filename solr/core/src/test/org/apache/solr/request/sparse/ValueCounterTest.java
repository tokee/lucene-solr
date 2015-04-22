package org.apache.solr.request.sparse;

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
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.packed.NPlaneMutable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedOpportunistic;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;


@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Appending"})
public class ValueCounterTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  public void testNPlaneThreaded() throws Exception {
    testNPlane(8);
  }

  public void testNPlaneNonThreaded() throws Exception {
    testNPlane(1);
  }

  public void testPackedOpportunisticReflectionThreaded() throws Exception {
    testPackedOpportunisticReflection(8);
  }

  // Generation of increments fails with -Dtests.seed=89A5D9C1D51AA973
  public void testPackedOpportunisticReflectionNonThreaded() throws Exception {
    testPackedOpportunisticReflection(1);
  }

  private void testNPlane(int threads) throws Exception {
    final int SIZE = 1000;
    final int MAX = 1000;
    final int MAX_UPDATES = SIZE*MAX;

    final PackedInts.Reader maxima = createMaxima(SIZE, MAX);
    SparseCounterThreaded counterA = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.nplane, new NPlaneMutable(maxima),
        MAX, 0, 1.0, -1);
    SparseCounterThreaded counterB = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.packed, PackedOpportunistic.create(SIZE, PackedInts.bitsRequired(MAX)),
        MAX, 0, 1.0, -1);

    updateAndTest(counterA, counterB, maxima, MAX_UPDATES, threads);
  }

  public void testNPlaneSmall() throws Exception {
    final int SIZE = 10;
    final int MAX = 10;
    final int MAX_UPDATES = 1;

    final PackedInts.Reader maxima = createMaxima(SIZE, MAX);
    SparseCounterThreaded counterA = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.nplane, new NPlaneMutable(maxima),
        MAX, 0, 1.0, -1);
    SparseCounterThreaded counterB = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.packed, PackedOpportunistic.create(SIZE, PackedInts.bitsRequired(MAX)),
        MAX, 0, 1.0, -1);

    updateAndTest(counterA, counterB, maxima, MAX_UPDATES, 1);
  }

  public void testNPlaneTrivial() throws Exception {
    final PackedInts.Mutable maxima = PackedInts.getMutable(10, 32, PackedInts.COMPACT);
    List<Long> maximaSrc= Arrays.asList(10L, 20L, 30L, 100L, 1L, 1L, 1L, 1L, 1L, 1L);
    for (int i = 0 ; i < maxima.size() ; i++) {
      maxima.set(i, maximaSrc.get(i));
    }

    SparseCounterThreaded counterA = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.nplane, new NPlaneMutable(maxima),
        100L, 0, 1.0, -1);

    counterA.inc(0);
    assertEquals("The value at index 0 should be correct", 1, counterA.get(0));
  }

  private void testPackedOpportunisticReflection(int threads) throws Exception {
    final int SIZE = 10;
    final int MAX = 1000;
    final int MAX_UPDATES = SIZE*MAX;

    final PackedInts.Reader maxima = createMaxima(SIZE, MAX);
    SparseCounterThreaded counterA = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.packed, PackedOpportunistic.create(SIZE, PackedInts.bitsRequired(MAX)),
        MAX, Integer.MAX_VALUE, 1.0, -1);
    SparseCounterThreaded counterB = new SparseCounterThreaded(
        SparseKeys.COUNTER_IMPL.packed, PackedOpportunistic.create(SIZE, PackedInts.bitsRequired(MAX)),
        MAX, Integer.MAX_VALUE, 1.0, -1);

    updateAndTest(counterA, counterB, maxima, MAX_UPDATES, threads);
  }

  private void updateAndTest(SparseCounterThreaded counterA, SparseCounterThreaded counterB, PackedInts.Reader maxima,
                             int maxUpdates, int threads) throws Exception {
    final long sum = sum(maxima);
    final PackedInts.Reader increments = generateRepresentativeValueIncrements(
        maxima, (int) Math.min(sum, maxUpdates), random().nextLong(), sum);

    if (threads == 1) {
      new UpdateJob(counterA, increments, maxima, 0, increments.size()).call();
      new UpdateJob(counterB, increments, maxima, 0, increments.size()).call();
    } else {
      final ExecutorService executor = Executors.newFixedThreadPool(threads * 2);
      int splitSize = increments.size() / threads;
      for (int i = 0; i < threads; i++) {
        executor.submit(new UpdateJob(counterA, increments, maxima, i * splitSize, splitSize));
        executor.submit(new UpdateJob(counterB, increments, maxima, i * splitSize, splitSize));
      }
      executor.shutdown();
    }
    assertWithinMaxima("counterA", maxima, counterA.counts);
    assertWithinMaxima("counterB", maxima, counterB.counts);
    assertVCEquals(counterB, counterA); // We trust PackedOpportunistic more
  }

  private void assertVCEquals(ValueCounter expected, ValueCounter actual) {
    assertEquals("The counters should have the same size", expected.size(), actual.size());
    for (int i = 0 ; i < expected.size() ; i++) {
      assertEquals("The values at index " + i + " should be equal", expected.get(i), actual.get(i));
    }
  }

  private PackedInts.Reader createMaxima(int count, int max) {
    PackedInts.Mutable maxima = PackedInts.getMutable(count, PackedInts.bitsRequired(max), PackedInts.COMPACT);
    for (int i = 0 ; i < count ; i++) {
      maxima.set(i, 1+random().nextInt(max-1));
    }
    return maxima;
  }

  private static PackedInts.Mutable generateRepresentativeValueIncrements(
      PackedInts.Reader maxima, int updates, long seed, long sum) {
    PackedInts.Mutable increments = PackedInts.getMutable
        (updates, PackedInts.bitsRequired(maxima.size()), PackedInts.FAST);
    if (maxima.size() < 1) {
      return increments;
    }

    final double delta = 1.0*sum/updates;
    double nextPos = 0; // Not very random to always start with 0...
    int currentPos = 1;
    long currentSum = maxima.get(0);
    out:
    for (int i = 0 ; i < updates ; i++) {
      while (nextPos > currentSum) {
        if (currentPos >= maxima.size()) {
          System.out.println(String.format(Locale.ENGLISH,
              "generateRepresentativeValueIncrements error: currentPos=%d with maxima.size()=%d at %d/%d updates",
              currentPos, maxima.size(), i+1, updates));
          break out; // Problem: This leaves the last counters dangling, potentially leading to overflow
        }
        currentSum += maxima.get(currentPos++);
      }
      increments.set(i, currentPos-1);
      nextPos += delta;
    }
    shuffle(increments, new Random(seed));
    verifyIncrements(maxima, increments);
    return increments;
  }

  private static void verifyIncrements(PackedInts.Reader maxima, PackedInts.Mutable increments) {
    PackedInts.Mutable counter = PackedInts.getMutable(maxima.size(), 32, PackedInts.FAST);
    //PackedInts.Mutable counter = PackedOpportunistic.create(maxima.size(), 32);
    // Fill
    for (int i = 0 ; i < increments.size() ; i++) {
      counter.set((int) increments.get(i), counter.get((int) increments.get(i))+1);
    }
    assertWithinMaxima("Verifying plain increments", maxima, increments);
  }

  private static void assertWithinMaxima(String message, PackedInts.Reader maxima, PackedInts.Reader counter) {
    for (int i = 0 ; i < maxima.size() ; i++) {
      assertTrue(message + ": counter(" + i + ")=" + counter.get(i)
              + " is greater than maxima(" + i + ")=" + maxima.get(i),
          counter.get(i) <= maxima.get(i));
    }
  }

  // http://stackoverflow.com/questions/1519736/random-shuffling-of-an-array
  private static void shuffle(PackedInts.Mutable values, Random random) {
    int index;
    long temp;
    for (int i = values.size() - 1; i > 0; i--) {
      index = random.nextInt(i + 1);
      temp = values.get(index);
      values.set(index, values.get(i));
      values.set(i, temp);
    }
  }

  public static long sum(PackedInts.Reader values) {
    long sum = 0;
    for (int i = 0 ; i < values.size() ; i++) {
      sum += values.get(i);
    }
    return sum;
  }

  private static class UpdateJob implements Callable<UpdateJob> {
    private final ValueCounter counters;
    private final PackedInts.Reader increments;
    private final PackedInts.Reader maxima;
    private final int start;
    private final int length;

    private UpdateJob(ValueCounter counters, PackedInts.Reader increments, PackedInts.Reader maxima,
                      int start, int length) {
      this.counters = counters;
      this.increments = increments;
      this.maxima = maxima;
      this.start = start;
      this.length = length;
    }

    @Override
    public UpdateJob call() throws Exception {
      for (int i = start; i < start + length; i++) {
        try {
//          System.out.println("inc(" + i + " -> " + increments.get(i) + ")");
          counters.inc((int) increments.get(i));
//          counters.set((int) increments.get(i), counters.get((int) increments.get(i))+1);
        } catch (Exception e) {
          int totalIncs = -1;
          for (int l = 0; l <= i; l++) { // Locate duplicate increments
            if (increments.get(l) == increments.get(i)) {
              totalIncs++;
            }
          }
          System.err.println(String.format(Locale.ENGLISH,
              "Exception calling %s.inc(%d) #%d with maximum=%d on %s. Aborting updates",
              counters, increments.get(i), totalIncs, maxima.get((int) increments.get(i)), counters));
          break;
        }
      }
      return this;
    }
  }

}

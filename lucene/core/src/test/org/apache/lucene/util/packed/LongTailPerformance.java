package org.apache.lucene.util.packed;

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


import org.apache.lucene.util.Incrementable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Non-unit-test performance test for using PackedInts implementations for counters
 * with long tail distributed maxima.
 */
public class LongTailPerformance {
  final static int M = 1048576;

  // The 2 tests are hard to activate, but also require manual inspection. Should they exist at all?

  public static void testSimplePerformance() {
    final int[] UPDATES = new int[] {1000, 10000};
    final int[] CACHES = new int[] {1000, 500, 200, 100, 50, 20};
    final int[] MAX_PLANES = new int[] {1, 2, 3, 4, 64};
    measurePerformance(pad(10000, 2000, 10, 3, 2, 1), 5, UPDATES, CACHES, MAX_PLANES);
  }

  // main(divisor "million updates" "N-plane 1/cache" "max planes")
  public static void main(String[] args) {
    final int RUNS = 9;
    int divisor = args.length == 0 ? 1 : Integer.parseInt(args[0]);
    int[] UPDATES = new int[] {M/10, M, 10*M, 100*M};
    if (args.length > 1) {
      String[] tokens = args[1].split(" ");
      UPDATES = new int[tokens.length];
      for (int i = 0 ; i < tokens.length ; i++) {
        UPDATES[i] = (int) (Double.parseDouble(tokens[i])*M);
      }
    }
    int[] CACHE = new int[]{1000, 500, 200, 100, 50, 20};
    if (args.length > 2) {
      String[] tokens = args[2].split(" ");
      CACHE = new int[tokens.length];
      for (int i = 0 ; i < tokens.length ; i++) {
        CACHE[i] = Integer.parseInt(tokens[i]);
      }
    }
    int[] MAX_PLANES = new int[]{64};
    if (args.length > 3) {
      String[] tokens = args[3].split(" ");
      MAX_PLANES = new int[tokens.length];
      for (int i = 0 ; i < tokens.length ; i++) {
        MAX_PLANES[i] = Integer.parseInt(tokens[i]);
      }
    }

    System.out.println(
        "Using divisor " + divisor + ", updates " + toString(UPDATES) + ", 1/cache " + toString(CACHE)
            + " and max planes " + toString(MAX_PLANES));
    measurePerformance(reduce(links20150209, divisor), RUNS, UPDATES, CACHE, MAX_PLANES);
  }

  static void measurePerformance(long[] histogram, int runs, int[] updates, int[] caches, int[] maxPlanes) {
    System.out.println("Creating pseudo-random maxima from histogram" + heap());
    final PackedInts.Reader maxima = getMaxima(histogram);
    histogram = getHistogram(maxima); // Re-calc as the maxima generator rounds up to nearest prime
    List<StatHolder> stats = new ArrayList<>();
    System.out.println("Initializing implementations" + heap());
//    int cache = NPlaneMutable.DEFAULT_OVERFLOW_BUCKET_SIZE;
    for (int cache : caches) {
      for (int mp: maxPlanes) {
        for (NPlaneMutable.IMPL impl: NPlaneMutable.IMPL.values()) {
          NPlaneMutable ltbpm =
              new NPlaneMutable(maxima, cache, mp, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, impl);
          stats.add(new StatHolder(
              ltbpm,
              "N-" + impl + "(#" + ltbpm.getPlaneCount() + ", 1/" + cache + ")",
              1
          ));
        }
      }
    }
    stats.add(new StatHolder(
        DualPlaneMutable.create(histogram, 0.99),
        "Dual-plane",
        1
    ));
    stats.add(new StatHolder(
        PackedInts.getMutable(maxima.size(), maxBit(histogram), PackedInts.COMPACT),
        "PackedInts.COMPACT",
        1
    ));
    stats.add(new StatHolder(
        PackedInts.getMutable(maxima.size(), maxBit(histogram), PackedInts.FAST),
        "PackedInts.FAST",
        1
    ));
/*    stats.add(new StatHolder(
        PackedInts.getMutable(maxima.size(), 31, PackedInts.FASTEST),
        "PackedInts int[]",
        updates
    ));*/

    PackedInts.Mutable valueIncrements = null;
    for (StatHolder stat: stats) {
      System.out.print(stat.designation + "  ");
    }
    System.out.println();

    for (int update: updates) {
      System.out.println(String.format("Performing %d test runs of %dM updates in %dM counters with max bit %d%s",
          runs, update / M, maxima.size() / 1000000, maxBit(histogram), heap()));

      for (StatHolder stat : stats) {
        stat.setUpdates(update);
      }
      for (int i = 0; i < runs; i++) {
        System.out.print("[generating update");
        final long seed = new Random().nextLong(); // Should really be random() but we want to run under main
        // Generate the increments to run
        valueIncrements = generateValueIncrements(maxima, update, valueIncrements, seed);
        System.gc(); // We don't want GC in the middle of measurements
        System.out.print("] ");
        for (StatHolder stat : stats) {
          stat.impl.clear();
          long ns = measure(stat.impl, valueIncrements, maxima);
          stat.addTiming(ns);
          System.out.print(String.format(Locale.ENGLISH, "%7d", (long) (((double) update) / ns * 1000000)));
        }
        System.out.println(heap());
      }
      for (StatHolder stat : stats) {
        System.out.println(stat);
        stat.addUPS(stat.getMedianUpdatesPerMS());
      }
    }
    // Overall stats
    System.out.print(String.format(Locale.ENGLISH,
        "<table style=\"width: 80%%\">" +
            "<caption>Median updates/ms of %dM counters with max bit %d</caption>\n" +
            "<tr style=\"text-align: right\"><th>Implementation</th> <th>MB</th>",
        maxima.size() / 1000000, maxBit(histogram)));
    for (int update: updates) {
      System.out.print(String.format(Locale.ENGLISH, " <th>%s updates</th>", update >= M ? update/M + "M" : update));
    }
    System.out.println("</tr>");
    for (StatHolder stat: stats) {
      System.out.print(String.format(Locale.ENGLISH,
          "<tr style=\"text-align: right;\"><th>%s</th> <td>%d</td>",
          stat.designation, stat.impl.ramBytesUsed()/M));
      for (int i = 0 ; i < updates.length ; i++) {
        System.out.print(String.format(Locale.ENGLISH, " <td>%.0f</td>", stat.ups.get(i)));
      }
      System.out.println("</tr>");
    }
    System.out.println("</table>");
  }

  private static PackedInts.Mutable generateValueIncrements(
      PackedInts.Reader maxima, int updates, PackedInts.Mutable increments, long seed) {
    if (increments != null && increments.size() == updates) {
      increments.clear();
    } else {
      increments = PackedInts.getMutable(updates, PackedInts.bitsRequired(maxima.size()), PackedInts.FAST);
    }
    final Random random = new Random(seed);
    final PackedInts.Mutable tracker =
        PackedInts.getMutable(maxima.size(), maxima.getBitsPerValue(), PackedInts.FAST);

    for (int i = 0 ; i < updates ; i++) {
      int index = random.nextInt(maxima.size());
      while (tracker.get(index) == maxima.get(index)) {
        if (++index == maxima.size()) {
          index = 0;
        }
      }
      tracker.set(index, tracker.get(index)+1);
      increments.set(i, index);
    }
    return increments;
  }

  // Runs a performance test and reports time spend as nano seconds
  private static long measure(
      PackedInts.Mutable counters, PackedInts.Reader valueIncrements, PackedInts.Reader maxima) {
    final Incrementable incCounters = counters instanceof Incrementable ?
        (Incrementable)counters :
        new Incrementable.IncrementableMutable(counters);

    long start = System.nanoTime();
    for (int i = 0 ; i < valueIncrements.size() ; i++) {
      try {
        incCounters.inc((int) valueIncrements.get(i));
      } catch (Exception e) {
        int totalIncs = 0;
        for (int l = 0 ; l <= i ; l++) {
          if (valueIncrements.get(l) == valueIncrements.get(i)) {
            totalIncs++;
          }
        }
        throw new RuntimeException(String.format(Locale.ENGLISH,
            "Exception calling inc(%d) #%d with maximum=%d",
            valueIncrements.get(i), totalIncs, maxima.get((int) valueIncrements.get(i))));
      }
    }
    return System.nanoTime()-start;
  }

  // histogram[0] = first bit
  public static long[] getHistogram(int[] maxima) {
    final long[] histogram = new long[64];
    for (int maxValue : maxima) {
      int bitsRequired = PackedInts.bitsRequired(maxValue);
      histogram[bitsRequired == 0 ? 0 : bitsRequired - 1]++;
    }
    return histogram;
  }
  public static long[] getHistogram(PackedInts.Reader maxima) {
    final long[] histogram = new long[64];
    for (int i = 0 ; i < maxima.size() ; i++) {
      int bitsRequired = PackedInts.bitsRequired(maxima.get(i));
      histogram[bitsRequired == 0 ? 0 : bitsRequired - 1]++;
    }
    return histogram;
  }

  private static class StatHolder {
    private final PackedInts.Mutable impl;
    private final String designation;
    private final List<Long> timings = new ArrayList<>();
    private int updatesPerTiming;
    private final List<Double> ups = new ArrayList<>();

    public StatHolder(PackedInts.Mutable impl, String designation, int updatesPerTiming) {
      this.impl = impl;
      this.designation = designation;
      this.updatesPerTiming = updatesPerTiming;
      System.out.println("Created StatHolder: " + impl.getClass().getSimpleName() + ": " + designation + " ("
      + impl.ramBytesUsed()/M + "MB)" + heap());
    }

    public void addTiming(long ns) {
      timings.add(ns);
    }

    public void addUPS(double ups) {
      this.ups.add(ups);
    }

    public double getMedianUpdatesPerMS() {
      Collections.sort(timings);
      return timings.isEmpty() ? 0 : ((double)updatesPerTiming)/timings.get(timings.size()/2)*1000000;
    }

    public String toString() {
      return String.format("%-22s (%3dMB): %6d updates/ms median",
          designation, impl.ramBytesUsed()/M, (long)getMedianUpdatesPerMS());
    }

    public void setUpdates(int updates) {
      updatesPerTiming = updates;
      timings.clear();
    }
  }

  public static final class PackedWrapped extends PackedInts.ReaderImpl {
    private final int[] values;

    public PackedWrapped(int[] values) {
      super(values.length, 32);
      this.values = values;
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF) +
          RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 4*values.length;
    }

    @Override
    public long get(int docID) {
      return values[docID];
    }
  }

  // Convert to int and cut zeroes at end
  public static int[] toGeneratorHistogram(long[] values) {
    int max = 0;
    for (int i = 0 ; i < values.length ; i++) {
      if (values[i] != 0) {
        max = i;
      }
    }
    final int[] ints = new int[max+1];
    for (int i = 0 ; i <= max ; i++) {
      ints[i] = (int)values[i];
    }
    return ints;
  }

  public static int maxBit(long[] histogram) {
    int maxBit = 0;
    for (int i = 0 ; i < histogram.length ; i++) {
      if (histogram[i] != 0) {
        maxBit = i+1; // Counting from 0
      }
    }
    return maxBit;
  }

  private static String heap() {
    Runtime runtime = Runtime.getRuntime();
    return " (" + (runtime.totalMemory() - runtime.freeMemory()) / M + "/" +
        runtime.maxMemory()/ M + "MB heap used)";
  }

  // Index 0 = first bit
  public static PackedInts.Reader getMaxima(long[] histogram) {
    return new PackedWrapped(
        LongTailIntGenerator.generateFromBitHistogram(toGeneratorHistogram(histogram), 1000));

  }

  public static long[] pad(long... maxCounts) {
    long[] full = new long[64];
    System.arraycopy(maxCounts, 0, full, 0, maxCounts.length);
    return full;
  }

  private static String toString(int[] values) {
    StringBuilder sb = new StringBuilder();
    for (int v: values) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(v >= M ? v/M + "M" : v);
    }
    return sb.toString();
  }

  public static long[] reduce(long[] values, int divisor) {
    final long[] result = new long[values.length];
    for (int i = 0 ; i < values.length ; i++) {
      result[i] = values[i] / divisor;
    }
    return result;
  }

  public static final long[] links20150209 = pad( // Taken from a test-index in netarchive.dk with 217M docs / 906G
      425799733,
      85835129,
      52695663,
      33153759,
      18864935,
      10245205,
      5691412,
      3223077,
      1981279,
      1240879,
      714595,
      429129,
      225416,
      114271,
      45521,
      12966,
      4005,
      1764,
      805,
      789,
      123,
      77,
      1
  );
}

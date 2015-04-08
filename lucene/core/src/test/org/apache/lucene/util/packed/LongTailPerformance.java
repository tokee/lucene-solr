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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Non-unit-test performance test for using PackedInts implementations for counters
 * with long tail distributed maxima.
 */
public class LongTailPerformance {
  final static int M = 1048576;
  final static int MI = 1000000;
  public static void testSimplePerformance() {
    final int[] UPDATES = new int[] {1000, 10000};
    final int[] CACHES = new int[] {1000, 500, 200, 100, 50, 20};
    final int[] MAX_PLANES = new int[] {1, 2, 3, 4, 64};
    measurePerformance(pad(10000, 2000, 10, 3, 2, 1), 5, 5/2, 1, UPDATES, CACHES, MAX_PLANES, Integer.MAX_VALUE);
  }

  public static void main(String[] args) {
    for (String arg: args) {
      if ("-h".equals(arg)) {
        System.out.println(USAGE);
        return;
      }
    }
    int    RUNS =       toIntArray(getArgs(args, "-r", 9))[0];
    int    ENTRY =      toIntArray(getArgs(args, "-e", RUNS/2))[0];
    int    THREADS =    toIntArray(getArgs(args, "-t", Integer.MAX_VALUE))[0];
    int    INSTANCES =  toIntArray(getArgs(args, "-i", 1))[0];
    double[] UPDD =  toDoubleArray(getArgs(args, "-u", 0.1, 1, 10, 20));
    int[]  NCACHES =    toIntArray(getArgs(args, "-c", 1000, 500, 200, 100, 50, 20));
    int[]  MAX_PLANES = toIntArray(getArgs(args, "-p", 64));
    long[] HISTOGRAM = toLongArray(getArgs(args, "-m", toString(links20150209).split(", ")));
    double FACTOR =  toDoubleArray(getArgs(args, "-d", 1.0))[0];

    int[] UPDATES = new int[UPDD.length];
    for (int i = 0; i < UPDD.length; i++) {
      UPDATES[i] = (int) (UPDD[i]*MI);
    }
    HISTOGRAM = reduce(pad(HISTOGRAM), 1/FACTOR);
    System.out.println(String.format(Locale.ENGLISH,
        "LongTailPerformance: runs=%d, entry=%d, threads=%s, instances=%d, updates=[%s], ncaches=[%s]," +
            " nmaxplanes=[%s], histogram=[%s](factor=%4.2f)",
        RUNS, ENTRY, THREADS == Integer.MAX_VALUE ? "unlimited" : THREADS, INSTANCES, join(UPDATES),
        join(NCACHES), join(MAX_PLANES), join(HISTOGRAM), FACTOR));
    measurePerformance(HISTOGRAM, RUNS, ENTRY, INSTANCES, UPDATES, NCACHES, MAX_PLANES, THREADS);
  }

  static void measurePerformance(
      long[] histogram, int runs, int entry, int instances, int[] updates,
      int[] caches, int[] maxPlanes, int threads) {
    System.out.println("Creating pseudo-random maxima from histogram" + heap());
    final PackedInts.Reader maxima = getMaxima(histogram);
    histogram = getHistogram(maxima); // Re-calc as the maxima generator rounds up to nearest prime
    List<StatHolder> stats = new ArrayList<>();
    System.out.println("Initializing implementations" + heap());
//    int cache = NPlaneMutable.DEFAULT_OVERFLOW_BUCKET_SIZE;
    char id = 'a';
    for (int d = 0 ; d < instances ; d++) {
      for (int mp : maxPlanes) {
        NPlaneMutable.Layout layout = null;
        for (int cache : caches) {
          layout = NPlaneMutable.getLayout(
              new NPlaneMutable.BPVPackedWrapper(maxima, false), cache, mp, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION);
          for (NPlaneMutable.IMPL impl : new NPlaneMutable.IMPL[]{NPlaneMutable.IMPL.split, NPlaneMutable.IMPL.shift}) {
            NPlaneMutable nplane = new NPlaneMutable(layout, new NPlaneMutable.BPVPackedWrapper(maxima, false), impl);
            stats.add(new StatHolder(nplane, id++,
                "N-" + impl + "(#" + nplane.getPlaneCount() + ", 1/" + cache + ")",
                1));
          }
        }
        NPlaneMutable nplane = layout == null ?
            new NPlaneMutable(new NPlaneMutable.BPVPackedWrapper(maxima, false), 0, mp,
                NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, NPlaneMutable.IMPL.spank) :
            new NPlaneMutable(layout, new NPlaneMutable.BPVPackedWrapper(maxima, false), NPlaneMutable.IMPL.spank);
        stats.add(new StatHolder(nplane, id++,
            "N-" + NPlaneMutable.IMPL.spank + "(#" + nplane.getPlaneCount() + ")",
            1));
      }
      stats.add(new StatHolder(
          DualPlaneMutable.create(histogram, 0.99), id++,
          "Dual-plane",
          1));
      stats.add(new StatHolder(
          PackedInts.getMutable(maxima.size(), maxBit(histogram), PackedInts.COMPACT), id++,
          "PackedInts.COMPACT",
          1));
      stats.add(new StatHolder(
          PackedInts.getMutable(maxima.size(), maxBit(histogram), PackedInts.FAST), id,
          "PackedInts.FAST",
          1));
/*    stats.add(new StatHolder(
        PackedInts.getMutable(maxima.size(), 31, PackedInts.FASTEST),
        "PackedInts int[]",
        updates
    ));*/
    }
    for (StatHolder stat: stats) {
      System.out.print(stat.id + ":" + stat.designation + "  ");
    }
    System.out.println();

    measure(runs, entry, threads, updates, histogram, maxima, stats);
    // Overall stats
    System.out.print(String.format(Locale.ENGLISH,
        "<table style=\"width: 80%%\">" +
            "<caption>Entry %d/%d increasing order updates/ms of %dM counters with max bit %d, using %d threads" +
            "</caption>\n<tr style=\"text-align: right\"><th>Implementation</th> <th>MB</th>",
        entry, runs, maxima.size() / 1000000, maxBit(histogram), threads));

    for (int update: updates) {
      System.out.print(String.format(Locale.ENGLISH, " <th>%s updates</th>", update >= MI ? update/MI + "M" : update));
    }
    System.out.println("</tr>");
    for (StatHolder stat: stats) {
      System.out.print(String.format(Locale.ENGLISH,
          "<tr style=\"text-align: right;\"><th style=\"align: left;\">%s</th> <td>%d</td>",
          stat.designation, stat.impl.ramBytesUsed()/M));
      for (int i = 0 ; i < updates.length ; i++) {
        System.out.print(String.format(Locale.ENGLISH, " <td>%.0f</td>", stat.ups.get(i)));
      }
      System.out.println("</tr>");
    }
    System.out.println("</table>");
  }

  private static void measure(int runs, int entry, int threads, int[] updates, long[] histogram,
                              PackedInts.Reader maxima, List<StatHolder> stats) {
    PackedInts.Mutable valueIncrements = null; // For re-use
    final long sum = sum(maxima); // For generation of increments
    final ExecutorService executor = Executors.newFixedThreadPool(Math.min(threads, stats.size()));

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
        valueIncrements = generateRepresentativeValueIncrements(maxima, update, valueIncrements, seed, sum);
        System.gc(); // We don't want GC in the middle of measurements
        System.out.print("] ");
        List<Future<StatHolder>> statFutures = new ArrayList<>(stats.size());
        for (StatHolder stat : stats) {
          stat.impl.clear();
          stat.setIncrements(valueIncrements, maxima);
          statFutures.add(executor.submit(stat));
        }
        for (Future<StatHolder> statFuture: statFutures) {
          try {
            StatHolder statHolder = statFuture.get();
            System.out.print(String.format(Locale.ENGLISH, "%c:%5d  ",
                statHolder.id, (long) (((double) statHolder.updatesPerTiming) / statHolder.lastNS * 1000000)));

          } catch (Exception e) {
            throw new RuntimeException("Unexpected exception while waiting for test to finish", e);
          }
        }
        System.out.println(heap());
      }
      for (StatHolder stat : stats) {
        System.out.println(stat);
        stat.addUPS(stat.getUpdatesPerMS(entry));
      }
    }
    executor.shutdownNow();
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

  // The higher the maxima, the more times it is to be incremented. Randomness is replaced with even distribution
  private static PackedInts.Mutable generateRepresentativeValueIncrements(
      PackedInts.Reader maxima, int updates, PackedInts.Mutable increments, long seed, long sum) {
    if (increments != null && increments.size() == updates) {
      increments.clear();
    } else {
      increments = PackedInts.getMutable(updates, PackedInts.bitsRequired(maxima.size()), PackedInts.FAST);
    }
    if (maxima.size() < 1) {
      return increments;
    }

    final double delta = 1.0*sum/updates;
    double nextPos = 0; // Not very random to always start with 0...
    int currentPos = 1;
    long currentSum = maxima.get(0);
    for (int i = 0 ; i < updates ; i++) {
      while (nextPos > currentSum) {
        currentSum += maxima.get(currentPos++);
      }
      increments.set(i, currentPos-1);
      nextPos += delta;
      if (nextPos >= maxima.size()) {
        System.out.println(String.format(Locale.ENGLISH,
            "generateRepresentativeValueIncrements error: nextPos=%f with maxima.size()=%d. Rounding down",
            nextPos, maxima.size()));
      }
    }
    shuffle(increments, new Random(seed));
    // shuffle
    return increments;
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
        int totalIncs = -1;
        for (int l = 0 ; l <= i ; l++) { // Locate duplicate increments
          if (valueIncrements.get(l) == valueIncrements.get(i)) {
            totalIncs++;
        }
        }
        throw new RuntimeException(String.format(Locale.ENGLISH,
            "Exception calling inc(%d) #%d with maximum=%d and #counters=%d",
            valueIncrements.get(i), totalIncs, maxima.get((int) valueIncrements.get(i)), counters.size()), e);
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
  private static class StatHolder implements Callable<StatHolder> {

    private final PackedInts.Mutable impl;
    private final String designation;
    private final List<Long> timings = new ArrayList<>();
    private int updatesPerTiming;
    private final List<Double> ups = new ArrayList<>();
    private final char id;
    private PackedInts.Reader increments;
    private long lastNS = -1;

    private PackedInts.Reader maxima;
    public StatHolder(PackedInts.Mutable impl, char id, String designation, int updatesPerTiming) {
      this.impl = impl;
      this.id = id;
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

    public double getUpdatesPerMS(int entry) {
      Collections.sort(timings);
      Collections.reverse(timings);
      return timings.isEmpty() ? 0 : ((double)updatesPerTiming)/timings.get(entry)*1000000;
    }

    public String toString() {
      return String.format("%-22s (%3dMB): %6d updates/ms median, %6d updates/ms max",
          id + ": " + designation, impl.ramBytesUsed()/M,
          (long)getUpdatesPerMS(timings.size()/2), (long)getUpdatesPerMS(timings.size()-1));
    }

    public void setUpdates(int updates) {
      updatesPerTiming = updates;
      timings.clear();
    }

    // Test code below

    public void setIncrements(PackedInts.Reader increments, PackedInts.Reader maxima) {
/*      PackedInts.Mutable clone = PackedInts.getMutable(
          increments.size(), increments.getBitsPerValue(), PackedInts.DEFAULT);
      for (int i = 0 ; i < increments.size() ; i++) {
        clone.set(i, increments.get(i));
      }
      this.increments = clone;*/
      this.increments = increments; // clone takes too much memory to be feasible
      this.maxima = maxima;
    }


    @Override
    public StatHolder call() throws Exception {
      long ns = measure(impl, increments, maxima);
      addTiming(ns);
      this.lastNS = ns;
//      System.out.print(String.format(Locale.ENGLISH, "%c:%5d  ",
//          id, (long) (((double) updatesPerTiming) / ns * 1000000)));
      return this;
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
  private static String toString(long[] values) {
    StringBuilder sb = new StringBuilder();
    for (long v: values) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(v);
    }
    return sb.toString();
  }

  public static long[] reduce(long[] values, double divisor) {
    final long[] result = new long[values.length];
    for (int i = 0 ; i < values.length ; i++) {
      result[i] = (long) (values[i] / divisor);
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

  private static String join(int[] values) {
    StringBuilder sb = new StringBuilder(values.length*5);
    for (int value: values) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(Integer.toString(value));
    }
    return sb.toString();
  }

  private static String join(long[] values) {
    StringBuilder sb = new StringBuilder(values.length*5);
    for (long value: values) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(value));
    }
    return sb.toString();
  }
  private static int[] toIntArray(List<String> args) {
    int[] ints = new int[args.size()];
    for (int i = 0 ; i < args.size() ; i++) {
      ints[i] = Integer.parseInt(args.get(i));
    }
    return ints;
  }
  private static long[] toLongArray(List<String> args) {
    long[] longs = new long[args.size()];
    for (int i = 0 ; i < args.size() ; i++) {
      longs[i] = Long.parseLong(args.get(i));
    }
    return longs;
  }
  private static double[] toDoubleArray(List<String> args) {
    double[] doubles = new double[args.size()];
    for (int i = 0 ; i < args.size() ; i++) {
      doubles[i] = Double.parseDouble(args.get(i));
    }
    return doubles;
  }
  private static List<String> getArgs(String[] args, String option, Object... defaults) {
    if (defaults.length == 1 && defaults[0] instanceof List) {
      List vals = (List)defaults[0];
      defaults = vals.toArray();
    }
    List<String> values = new ArrayList<>();
    for (int i = 0 ; i < args.length ; i++) {
      if (args[i].equals(option)) {
        for (int j = i+1 ; j < args.length ; j++) {
          if (args[j].startsWith("-")) {
            break;
          }
          values.addAll(Arrays.asList(args[j].split(" +")));
        }
        if (values.isEmpty()) {
          throw new IllegalStateException("Must provide values for option '" + option + "'");
        }
        return values;
      }
    }
    for (Object o: defaults) {
      values.add(o.toString());
    }
    return values;
  }

  private static final String USAGE =
      "LongTailPerformance arguments\n" +
          "-h:    Display usage\n" +
          "-r x:  Number of runs per test case. Default: 9\n" +
          "-e x:  Which measurement to report, as an index in slowest to fastest run. Default: runs/2\n" +
          "-t x:  Number of Threads used per run. Default: Unlimited\n" +
          "-i x:  Duplicate all instances this number of times. Default: 1\n" +
          "-u x*: Number of million updates per run. Default: 0.1 1 10 20\n" +
          "-c x*: Cache-setups for N-plane. Default: 1000 500 200 111 50 20\n" +
          "-p x*: Max planes for N-plane. Default: 64\n" +
          "-m x*: Histogram maxima. Default: 425799733 85835129 52695663...\n" +
          "-d x:  Histogram multiplication factor. Default: 1.0\n\n" +
          "Note the absence of a warmup round. As the median over all runs is used, " +
          "the initial fluctuations of the JIT and the caches should not be irrelevant.";
}

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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
@Slow
public class TestDualPlaneMutable extends LuceneTestCase {

  private final static int M = 1048576;

  public void testLinksEstimate() { // 519*M
    testEstimate("8/9 shard links", getLinksHistogram(), false);
  }

  public void testURLEstimate() {
    testEstimate("Shard 1 URL", getURLShard1Histogram(), false);
    testEstimate("Shard 2 URL", getURLShard2Histogram(), false);
    testEstimate("Shard 3 URL", getURLShard3Histogram(), false);
  }
  public void testHostEstimate() { // 1562680
    testEstimate("Shard 1 host", SHARD1_HOST, false);
  }

  public void testViability() {
    DualPlaneMutable.Estimate estimate = new DualPlaneMutable.Estimate(100*M, pad(
        1000,
        100,
        1,
        3
    ));
    // 1+3=4 and there are 2^2=4 pointers
    assertTrue("The layout should be viable for tailBPV=2", estimate.isViable(2));
  }


  public void testMemoryUsages() {
    dumpImplementationMemUsages("Shard 1 URL", getURLShard1Histogram());
    dumpImplementationMemUsages("Links raw", getLinksHistogram());
    dumpImplementationMemUsages("Links 20150309", TestNPlaneMutable.links20150209);
    final long[] tegHistogram = getHistogram(LongTailIntGenerator.GenerateLongtailDistribution(640000000, 500000, 101));
    dumpImplementationMemUsages("TEG histogram generator", tegHistogram);
  }

  private void dumpImplementationMemUsages(String source, long[] histogram) {
    long valueCount = 0;
    for (long values: histogram) {
      valueCount += values;
    }
    final long intC = valueCount*4;
    final long packC = valueCount * maxBit(histogram) / 8;
    final long ltbpmC = NPlaneMutable.estimateBytesNeeded(histogram);
    final long ltbpmeC = NPlaneMutable.estimateBytesNeeded(histogram, true);
    final long ltmC = DualPlaneMutable.estimateBytesNeeded(histogram, (int) valueCount);
    long lowest = 0; // TODO: Something is wrong as this is not lowest
    for (int i = 0 ; i < histogram[i] ; i++) {
      lowest += histogram[i] * (i+1) / 8;
    }

    System.out.println(source + ": " + valueCount + " counters, max bit " + maxBit(histogram));
    System.out.println(String.format("Solr default int[]: %4dMB", intC/M));
    System.out.println(String.format("Sparse PackedInts:  %4dMB", packC/M));
    System.out.println(String.format("Long Tail Dual:     %4dMB", ltmC / M));
    System.out.println(String.format("Long Tail Planes:   %4dMB", ltbpmC / M));
    System.out.println(String.format("Long Tail Planes+:  %4dMB", ltbpmeC / M));
    System.out.println(String.format("Lowest possible:    %4dMB", lowest / M));
  }

  private static int maxBit(long[] histogram) {
    int maxBit = 0;
    for (int i = 0 ; i < histogram.length ; i++) {
      if (histogram[i] != 0) {
        maxBit = i+1; // Counting from 0
      }
    }
    return maxBit;
  }

  // main(divisor "million updates" "N-plane 1/cache")
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
    measurePerformance(reduce(TestNPlaneMutable.links20150209, divisor), RUNS, UPDATES, CACHE, MAX_PLANES);
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

  public void testSimplePerformance() {
    final int[] UPDATES = new int[] {1000, 10000};
    final int[] CACHES = new int[] {1000, 500, 200, 100, 50, 20};
    final int[] MAX_PLANES = new int[] {1, 2, 3, 4, 64};
    measurePerformance(pad(10000, 2000, 10, 3, 2, 1), 5, UPDATES, CACHES, MAX_PLANES);
  }

  public void testLargePerformance() {
    final int DIVISOR = 50;
    final int[] UPDATES = new int[] {M/10, M, 10*M, 100*M};
    final int[] CACHES = new int[] {1000, 500, 200, 100, 50, 20};
    final int[] MAX_PLANES = new int[] {4, 64};
    measurePerformance(reduce(TestNPlaneMutable.links20150209, DIVISOR), 9, UPDATES, CACHES, MAX_PLANES);
  }

  public static long[] reduce(long[] values, int divisor) {
    final long[] result = new long[values.length];
    for (int i = 0 ; i < values.length ; i++) {
      result[i] = values[i] / divisor;
    }
    return result;
  }

  private static void measurePerformance(long[] histogram, int runs, int[] updates, int[] caches, int[] maxPlanes) {
    System.out.println("Creating pseudo-random maxima from histogram" + heap());
    final PackedInts.Reader maxima = TestNPlaneMutable.getMaxima(histogram);
    List<StatHolder> stats = new ArrayList<>();
    System.out.println("Initializing implementations" + heap());
//    int cache = NPlaneMutable.DEFAULT_OVERFLOW_BUCKET_SIZE;
    for (int cache : caches) {
      for (int mp: maxPlanes) {
        NPlaneMutable ltbpm =
            new NPlaneMutable(maxima, cache, mp, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION);
        stats.add(new StatHolder(
            ltbpm,
            "N-plane(#" + ltbpm.getPlaneCount() + ", 1/" + cache + ")",
            1
        ));
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
          long ns = measure(stat.impl, valueIncrements);
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

  private static String heap() {
    Runtime runtime = Runtime.getRuntime();
    return " (" + (runtime.totalMemory() - runtime.freeMemory()) / M + "/" +
        runtime.maxMemory()/M + "MB heap used)";
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
  private static long measure(PackedInts.Mutable counters, PackedInts.Reader valueIncrements) {
    final Incrementable incCounters = counters instanceof Incrementable ?
        (Incrementable)counters :
        new Incrementable.IncrementableMutable(counters);

    long start = System.nanoTime();
    for (int i = 0 ; i < valueIncrements.size() ; i++) {
      incCounters.inc((int) valueIncrements.get(i));
    }
    return System.nanoTime()-start;
  }

  public void testNonViability() {
    DualPlaneMutable.Estimate estimate = new DualPlaneMutable.Estimate(100*M, pad(
        1000,
        100,
        2,
        3
    ));
    // 2+3=5 and there are 2^2=4 pointers
    assertFalse("The layout should not be viable for tailBPV=2", estimate.isViable(2));
  }

  public void testEstimate(String designation, long[] histogram, boolean table) {
    long uniqueCount = DualPlaneMutable.totalCounters(histogram);
    DualPlaneMutable.Estimate estimate = new DualPlaneMutable.Estimate(uniqueCount, histogram);
    final double PACKED_MB = uniqueCount*estimate.getMaxBPV()/8 / 1024.0 / 1024;
    System.out.println(String.format(Locale.ENGLISH,
        table ?
            "<table style=\"width: 80%%\"><caption>%s: %s uniques, Packed64 size: %.0fMB</caption>\n" +
                "<tr style=\"text-align: right\"><th>tailBPV</th> <th>mem</th> <th>saved</th> <th>headCounters</th></tr>" :
            "%s: %s uniques, Packed64 size: %.0fMB",
        designation, uniqueCount < 10*M ? uniqueCount/1000 + "K>" : uniqueCount/M + "M", PACKED_MB));
    for (int tailBPV = 0 ; tailBPV < 64 ; tailBPV++) {
      if (estimate.isViable(tailBPV) && estimate.getFractionEstimate(tailBPV) <= 1.0) {
        double mb = estimate.getMemEstimate(tailBPV) / 1024.0 / 1024;
        System.out.println(String.format(Locale.ENGLISH,
            table ?
                "<tr style=\"text-align: right\"><td>%2d</td> <td>%4.0fMB</td> <td>%3.0fMB / %2.0f%%</td> <td>%4d</td></tr>" :
                "tailBPV=%2d mem=%4.0fMB (%3.0fMB / %2.0f%% saved) headCounters=%4d (%6.4f%%)",
            tailBPV, mb, PACKED_MB-mb,
            // TODO: Split in fast and slow head
            (1-estimate.getFractionEstimate(tailBPV))*100, estimate.getHeadValueCount(tailBPV),
            estimate.getHeadValueCount(tailBPV)*100.0/uniqueCount));
      }
    }
    if (table) {
      System.out.println("</table>");
    }
  }

  public void testLongTailRandomGenerator() {
    final long[] histogram = getHistogram(LongTailIntGenerator.GenerateLongtailDistribution(300000000 , 500000,1000));
    System.out.println(toString(histogram, "\n"));
  }

  public void testLongTailExistingGenerator() {
    final long[] in = reduce(TestNPlaneMutable.links20150209, 10);
    System.out.println("*** Input histogram");
    System.out.println(toString(in, "\n"));

    final long[] histogram = getHistogram(LongTailIntGenerator.GenerateLongtailDistribution(toInt(in), 1000));
    System.out.println("*** Output histogram");
    System.out.println(toString(histogram, "\n"));
  }

  public static int[] toInt(long[] values) {
    final int[] ints = new int[values.length];
    for (int i = 0 ; i < values.length ; i++) {
      ints[i] = (int)values[i];
    }
    return ints;
  }

  public void testPrintRealWorldDistribution() {
    System.out.println(toString(getLinksHistogram(), "\n"));
  }

  private String toString(long[] histogram, String divider) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i <= maxBit(histogram); i++) {
      long maxValue = histogram[i];
      if (sb.length() > 0) {
        sb.append(divider);
      }
      sb.append(String.format("Bit %2d: %9d", i+1, maxValue));
    }
    return sb.toString();
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
  public static long[] getHistogram(long[] maxima) {
    final long[] histogram = new long[64];
    for (long maxValue : maxima) {
      int bitsRequired = PackedInts.bitsRequired(maxValue);
      histogram[bitsRequired == 0 ? 0 : bitsRequired - 1]++;
    }
    return histogram;
  }

  private long[] getURLShard1Histogram() { // Taken from URL from shard 1 in netarchive.dk
    // 228M uniques
    return pad(
        196552211,
        20504581,
        5626432,
        3187738,
        1123002,
        411353,
        164206,
        81438,
        10421, // 256
        2252,
        2526,
        100,
        6      // 4096
    );
  }

  private long[] SHARD1_HOST = pad(
      // 1562680 uniques
      194715,
      181562,
      178020,
      177279,
      168170,
      129294,
      90654,
      60348,
      39070,
      22778,
      13934,
      8515,
      5829,
      3876,
      1799,
      556,
      153,
      44,
      22,
      17,
      6,
      3,
      1
  );

  private long[] getURLShard2Histogram() { // Taken from URL in netarchive.dk
    // 230M uniques
    return pad(
        213596462,
        7814717,
        3496320,
        2446379,
        1370559,
        474296,
        207471,
        91941,
        18593,
        3467,
        2754,
        150,
        7
    );
  }
  private long[] getURLShard3Histogram() { // Taken from URL in netarchive.dk
    // 214M uniques
    return pad(
        188429984,
        17107714,
        4977514,
        2431354,
        946224,
        361199,
        140717,
        65064,
        6183,
        2278,
        1996,
        64,
        3
    );
  }

  /*

  0000
  0000

  1000
  0000

  0000
  1000
  100
  000

  1000
  1000
  100
  000

  0000
  1000
  000
  100
  10
  00

   */

  public void testBitPlanePacking() {
    final long VALUES = 519*M;
    final long[] histogram = getLinksHistogram();

    double totalBits = 2*VALUES; // Base
    System.out.println(String.format("hist=%10d, bits=%d, total=%dMB", VALUES, 0, (int)(totalBits/8/M)));

    for (int bits = 1 ; bits < 64 ; bits++) {
      totalBits += 2 * histogram[bits-1];
      System.out.println(String.format("hist=%10d, bits=%d, total=%dMB", histogram[bits-1], bits, (int)(totalBits/8/M)));
      if (histogram[bits-1] == 0) {
        break; // Shouldn't this reach 0 automatically?
      }
    }

    System.out.println(String.format("%dMB/%dMB: %4.2f", (long)(totalBits/8/M), VALUES*4/M, totalBits / (VALUES*32)));
  }

  // if (histogram[bits+1] < histogram[bits]/2) collapse

  /**
   * Current optimal space solution: Keep 1 bitplane (bitmap) for every bit in the number and 1 bitplane for
   * signaling overflow. If a subsequent bitplane is more than half the size of the previous one, the two will
   * share overflow bit.
   * </p><p>
   * As the overflow bits must be counted to infer the index of the next bit position for a given counter,
   * the raw version is extremely slow, requiring billions of bit-checks to update a single counter.
   */
  public void testMultiBitPlanePacking() {
    final long VALUES = 519*M;
    final long[] histogram = getLinksHistogram();

    final long[] values = new long[histogram.length+2];
    System.arraycopy(histogram, 0, values, 0, histogram.length);
    values[0] = 519*M;

    double totalBits = 0;
    int bits = 0;
    while (bits < 64) {
      long bitmapLength = values[bits];
      totalBits += bitmapLength;
      if (values[bits+1] < values[bits]/2) { // share overflow bits
        System.out.println(String.format(
            "Collapsing bit %d+%d (%d, %d values)", bits, bits+1, values[bits], values[bits+1]));
        totalBits += bitmapLength;
        bits++;
      } else if (values[bits] != 0) {
        System.out.println(String.format(
            "Plain storage of bit %d (%d values)", bits, values[bits]));
      }
      totalBits += bitmapLength;
      bits++;
    }
    System.out.println(String.format("%dMB/%dMB: %4.2f", (long)(totalBits/8/M), VALUES*4/M, totalBits / (VALUES*32)));
  }

  public static long[] getLinksHistogram() {
    return pad( // Taken from links in a 8/9 build shard from netarchive.dk
        // 519M uniques
        351962313,
        64785381,
        42315979,
        26072745,
        14361240,
        8035509,
        4611177,
        2686949,
        1659800,
        1005294,
        632518,
        368445,
        208885,
        94266,
        32975,
        8196,
        2807,
        1755,
        465,
        540,
        311,
        58
    );
  }

  public static long[] pad(long... maxCounts) {
    long[] full = new long[64];
    System.arraycopy(maxCounts, 0, full, 0, maxCounts.length);
    return full;
  }
}

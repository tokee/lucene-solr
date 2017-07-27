package org.apache.solr.search.sparse.packed;

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


import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.BPVProvider;
import org.apache.solr.search.sparse.count.DualPlaneMutable;
import org.apache.solr.search.sparse.count.Incrementable;
import org.apache.solr.search.sparse.count.PackedOpportunistic;
import org.apache.solr.search.sparse.count.plane.NPlaneLayout;
import org.apache.solr.search.sparse.count.plane.NPlaneMutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Non-unit-test performance test for using PackedInts implementations for counters
 * with long tail distributed maxima.
 */
public class LongTailPerformance {
  final static int M = 1048576;
  final static int MI = 1000000;
  public static void testSimplePerformance() {
    final int UPDATES = 10000;
    final int[] CACHES = new int[] {1000, 500, 200, 100, 50, 20};
    final int[] MAX_PLANES = new int[] {1, 2, 3, 4, 64};
    final int[] SPLITS = new int[] {1};
    measurePerformance(
        pad(10000, 2000, 10, 3, 2, 1), 5, 5/2, 1, UPDATES, 10, CACHES, MAX_PLANES, Integer.MAX_VALUE, SPLITS, false, null);
  }

  public static void main(String[] args) {
    for (String arg: args) {
      if ("-h".equals(arg)) {
        System.out.println(USAGE);
        return;
      }
    }
    int    RUNS =       toIntArray(getArgs(args, "-r", 5))[0];
    int    ENTRY =      toIntArray(getArgs(args, "-e", RUNS/2))[0];
    int    THREADS =    toIntArray(getArgs(args, "-t", Integer.MAX_VALUE))[0];
    int[]  SPLITS =     toIntArray(getArgs(args, "-s", 1));
    int    INSTANCES =  toIntArray(getArgs(args, "-i", 1))[0];
    double UPDD =    toDoubleArray(getArgs(args, "-u", 10))[0];
    int MEASURE_POINTS= toIntArray(getArgs(args, "-a", 50))[0];
    int[]  NCACHES =    toIntArray(getArgs(args, "-c", 1000, 500, 200, 100, 50, 20));
    int[]  MAX_PLANES = toIntArray(getArgs(args, "-p", 64));
    long[] HISTOGRAM = toLongArray(getArgs(args, "-m", toString(links20150209).split(", ")));
    double FACTOR =  toDoubleArray(getArgs(args, "-d", 1.0))[0];

    int UPDATES = (int) (UPDD*MI);
    HISTOGRAM = reduce(pad(HISTOGRAM), 1/FACTOR);
    System.out.println(String.format(Locale.ENGLISH,
        "LongTailPerformance: runs=%d, entry=%d, threads=%s, splits=%s, instances=%d, updates=%d, ncaches=[%s]," +
            " nmaxplanes=[%s], histogram=[%s](factor=%4.2f)",
        RUNS, ENTRY, THREADS == Integer.MAX_VALUE ? "unlimited" : THREADS, join(SPLITS), INSTANCES, UPDATES,
        join(NCACHES), join(MAX_PLANES), join(HISTOGRAM), FACTOR));
    measurePerformance(HISTOGRAM, RUNS, ENTRY, INSTANCES, UPDATES, MEASURE_POINTS, NCACHES, MAX_PLANES, THREADS,
        SPLITS, false, null);
  }

  public static void measurePerformance(
      long[] inHistogram, int runs, int entry, int instances, int updates, int measurePoints,
      int[] caches, int[] maxPlanes, int threads, int[] splits, boolean checkEquivalence, char[] whitelist) {
    System.out.println("Creating pseudo-random maxima from histogram" + heap());
    final PackedInts.Reader maxima = getMaxima(inHistogram);
    BPVProvider.StatCollectingBPVWrapper bitStats = new BPVProvider.StatCollectingBPVWrapper(
        new BPVProvider.BPVPackedWrapper(maxima, false));
    bitStats.collect();

    List<StatHolder> stats = new ArrayList<>();
    System.out.println("Initializing implementations" + heap());
//    int cache = NPlaneMutable.DEFAULT_OVERFLOW_BUCKET_SIZE;

    // Create the instances
    char id = 'a';
    for (int d = 0; d < instances; d++) {
      for (int split : splits) { // Counters that support threaded updates (currently only tank)
        if (acceptID(whitelist, id++)) {
          stats.add(new StatHolder(
              new DummyMutable(maxima.size()), id, "Dummy(s=" + split + ")", updates, measurePoints, split));
        }
      }
      for (int mp : maxPlanes) {
        NPlaneLayout layout = null;
        NPlaneLayout layoutZero = null;
        for (int cache : caches) {
          layout = NPlaneLayout.getLayout(new BPVProvider.BPVPackedWrapper(maxima, false),
              cache, mp, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, false);
          layoutZero = NPlaneLayout.getLayout(new BPVProvider.BPVPackedWrapper(maxima, false),
              cache, mp, NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, true);
          // Disabled split as it is always worse than spank
//          for (NPlaneMutable.IMPL impl : new NPlaneMutable.IMPL[]{NPlaneMutable.IMPL.split, NPlaneMutable.IMPL.shift}) {
          for (NPlaneMutable.IMPL impl : new NPlaneMutable.IMPL[]{NPlaneMutable.IMPL.shift}) {
            if (acceptID(whitelist, id++)) {
              NPlaneMutable nplane = new NPlaneMutable(layout, new BPVProvider.BPVPackedWrapper(maxima, false), impl);
              stats.add(new StatHolder(nplane, id,
                  "N-" + impl + "(#" + nplane.getPlaneCount() + ", 1/" + cache + ")", updates, measurePoints, 1));
            }
          }
        }
        for (int split : splits) { // Counters that support threaded updates (currently only tank)
          if (acceptID(whitelist, id++)) {
            NPlaneMutable nplane = layout == null ?
                new NPlaneMutable(new BPVProvider.BPVPackedWrapper(maxima, false), 0, mp,
                    NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, NPlaneMutable.IMPL.tank) :
                new NPlaneMutable(layout, new BPVProvider.BPVPackedWrapper(maxima, false), NPlaneMutable.IMPL.tank);
            stats.add(new StatHolder(nplane, id,
                "N-" + NPlaneMutable.IMPL.tank + "(#" + nplane.getPlaneCount() + ", s=" + split + ")",
                updates, measurePoints, split));
          }
        }
        if (acceptID(whitelist, id++)) {
          NPlaneMutable nplane = layout == null ?
              new NPlaneMutable(new BPVProvider.BPVPackedWrapper(maxima, false), 0, mp,
                  NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, NPlaneMutable.IMPL.spank) :
              new NPlaneMutable(layout, new BPVProvider.BPVPackedWrapper(maxima, false), NPlaneMutable.IMPL.spank);
          stats.add(new StatHolder(nplane, id,
              "N-" + NPlaneMutable.IMPL.spank + "(#" + nplane.getPlaneCount() + ")", updates, measurePoints, 1));
        }
        if (acceptID(whitelist, id++)) {
          NPlaneMutable nplane = layoutZero == null ?
              new NPlaneMutable(new BPVProvider.BPVPackedWrapper(maxima, false), 0, mp,
                  NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, NPlaneMutable.IMPL.zethra) :
              new NPlaneMutable(layoutZero, new BPVProvider.BPVPackedWrapper(maxima, false),
                  NPlaneMutable.IMPL.zethra);
          stats.add(new StatHolder(nplane, id,
              "N-" + NPlaneMutable.IMPL.zethra + "(#" + nplane.getPlaneCount() + ")", updates, measurePoints, 1));
        }
      }
      if (acceptID(whitelist, id++)) {
        stats.add(new StatHolder(
            DualPlaneMutable.create(bitStats.histogram, 0.99), id,
            "Dual-plane", updates, measurePoints, 1));
      }
      if (acceptID(whitelist, id++)) {
        stats.add(new StatHolder(
            PackedInts.getMutable(maxima.size(), maxBit(bitStats.histogram), PackedInts.COMPACT), id,
            "PackedInts.COMPACT", updates, measurePoints, 1));
      }
      for (int split : splits) { // Counters that support threaded updates (currently only tank)
        for (int candidateBPV = maxBit(bitStats.histogram); candidateBPV < 64 ; candidateBPV++) {
          if (PackedOpportunistic.isSupported(candidateBPV)) {
            if (acceptID(whitelist, id++)) {
              stats.add(new StatHolder(
                  PackedOpportunistic.create(maxima.size(), candidateBPV), id,
                  "PackedOpport(s=" + split + ")", updates, measurePoints, split));
            }
            break;
          }
        }
      }
      if (acceptID(whitelist, id++)) {
        stats.add(new StatHolder(
            PackedInts.getMutable(maxima.size(), maxBit(bitStats.histogram), PackedInts.FAST), id,
            "PackedInts.FAST",
            updates, measurePoints, 1));
      }
/*    stats.add(new StatHolder(
        PackedInts.getMutable(maxima.size(), 31, PackedInts.FASTEST),
        "PackedInts int[]",
        updates
    ));*/
    }

    for (StatHolder stat : stats) {
      System.out.print(stat.id + ":" + stat.designation + "  ");
    }
    System.out.println();

    measure(runs, entry, threads, updates, measurePoints, maxBit(bitStats.histogram), maxima, stats, checkEquivalence);
    // Overall stats
    String caption = String.format(Locale.ENGLISH,
        "Increments/ms of %dM counters with max bit %d, using %d threads",
        maxima.size() / 1000000, maxBit(bitStats.histogram), threads);

    printHTMLTable(caption, updates, measurePoints, stats);
    printGnuplotData(caption, updates, measurePoints, entry, stats);
  }

  private static boolean acceptID(char[] whitelist, char id) {
    if (whitelist == null) {
      return true;
    }
    for (char ok : whitelist) {
      if (ok == id) {
        return true;
      }
    }
    return false;
  }

  private static void printHTMLTable(String caption, int updates, int measurePoints, List<StatHolder> stats) {
    // TODO: Flip the table
    /*
    System.out.print(String.format(Locale.ENGLISH,
        "\n<table style=\"width: 80%%\">" +
            "<caption>%s</caption>\n<tr style=\"text-align: right\"><th>Implementation</th> <th>MB</th>",
        caption));
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
    */
  }

  private static void printGnuplotData(
      String caption, int updates, int measurePoints, int entry, List<StatHolder> stats) {
    System.out.println("\n# " + caption);
    System.out.print("M_incs");
    for (StatHolder stat: stats) {
      System.out.print(String.format(Locale.ENGLISH, " %s(%d_MB)",
          stat.designation.replace(" ", "_"), stat.impl.ramBytesUsed()/M));
    }
    System.out.println();

    int delta = updates/measurePoints;
    int blocks = Integer.MAX_VALUE;
    for (StatHolder stat: stats) {
      blocks = Math.min(blocks, stat.getBlockCount());
    }
    for (int block = 0 ; block < blocks ; block++) {
      System.out.print(String.format(Locale.ENGLISH, "%.1f", 1.0*(block+1)*delta/MI));
      for (StatHolder stat: stats) {
        System.out.print(String.format(Locale.ENGLISH, " %.0f", stat.getUpdatesPerMS(block, entry)));
      }
      System.out.println("");
    }
  }

  private static void measure(int runs, int entry, int threads, int updates, int measurePoints, int maxBit,
                              PackedInts.Reader maxima, List<StatHolder> stats, boolean checkEquivalence) {
    PackedInts.Mutable valueIncrements = null; // For re-use
    final long sum = sum(maxima); // For generation of increments
    final ExecutorService executor = Executors.newFixedThreadPool(Math.min(threads, stats.size()));

    System.out.println(String.format("Performing %d test runs of %dM updates in %dM counters with max bit %d%s",
        runs, updates / MI, maxima.size() / 1000000, maxBit, heap()));

    // Set up the stats with the current test configuration
    // Run the tests
    for (int i = 0; i < runs; i++) {
      System.out.print("[generating update");
      final long seed = new Random().nextLong(); // Should really be random() but we want to run under main
      // Generate the increments to run
      valueIncrements = generateRepresentativeValueIncrements(maxima, updates, valueIncrements, seed, sum);
      System.gc(); // We don't want GC in the middle of measurements
      System.out.print("] ");
      List<Future<StatHolder>> statFutures = new ArrayList<>(stats.size());
      for (StatHolder stat : stats) {
        stat.prepareTest(valueIncrements, maxima);
        statFutures.add(executor.submit(stat));
      }
      for (Future<StatHolder> statFuture: statFutures) {
        try {
          StatHolder statHolder = statFuture.get();
          System.out.print(String.format(Locale.ENGLISH, "%c:%6.0f  ",
              statHolder.id, statHolder.getLastUpdatesPerMS(statHolder.getBlockCount()-1)));

        } catch (Exception e) {
          throw new RuntimeException("Unexpected exception while waiting for test to finish", e);
        }
      }
      System.out.println(heap());
    }
    executor.shutdownNow();

    if (checkEquivalence) {
      checkEquivalence(stats);
    }
  }

  private static void checkEquivalence(List<StatHolder> stats) {
    long errors = 0;
    Set<String> errorImpls = new HashSet<>();
    System.out.println("Checking equality of all " + stats.size() + " counters. Patience is a virtue");
    StatHolder base = stats.get(stats.size()-1);
    for (int shi = 0 ; shi < stats.size()-1 ; shi++) {
      StatHolder current = stats.get(shi);
      if (current.impl instanceof DummyMutable) {
        continue;
      }
      for (int i = 0 ; i < base.impl.size() ; i++) {
        if (base.impl.get(i) != current.impl.get(i)) {
          if (++errors < 50) {
            System.err.println(String.format("%s.get(%d) == %d. Expected %d",
                current.designation, i, current.impl.get(i), base.impl.get(i)));
          }
          errorImpls.add(current.designation);
        }
      }
    }
    System.out.println("Finished checking equivalence. Errors: " + errors + " in " + errorImpls);
  }

  private static PackedInts.Mutable generateValueIncrements(
      PackedInts.Reader maxima, int updates, PackedInts.Mutable increments, long seed) {
    if (increments != null && increments.size() == updates) {
      increments.clear();
    } else {
      increments = PackedInts.getMutable(updates, PackedInts.bitsRequired(maxima.size()), PackedInts.FAST);
    }
    final Random random = new Random(seed);
    final PackedInts.Mutable tracker =PackedInts.getMutable(maxima.size(), 32, PackedInts.FAST);

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
    out:
    for (int i = 0 ; i < updates ; i++) {
      while (nextPos + 0.01 >= currentSum) { // 0.01 to handle rounding errors
        if (currentPos >= maxima.size()) {
          System.out.println(String.format(Locale.ENGLISH,
              "generateRepresentativeValueIncrements error: currentPos=%d with maxima.size()=%d at %d/%d updates",
              currentPos, maxima.size(), i+1, updates));
          break out; // Problem: This leaved the last counters dangling, potentially leading to overflow
        }
        currentSum += maxima.get(currentPos++);
      }
      increments.set(i, currentPos-1);
      nextPos += delta;
    }
    shuffle(increments, new Random(seed));
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
  private static long measure(PackedInts.Mutable counters, PackedInts.Reader valueIncrements, PackedInts.Reader maxima,
                              int splits, StatHolder statHolder) {
    final Incrementable incCounters = counters instanceof Incrementable ?
        (Incrementable)counters :
        new Incrementable.IncrementableMutable(counters);

    long start = System.nanoTime();
    if (splits == 1 || valueIncrements.size() < splits) {
      UpdateJob uf = new UpdateJob(incCounters, valueIncrements, maxima, 0, valueIncrements.size(), statHolder);
      try {
        uf.call();
      } catch (Exception e) {
        throw new RuntimeException("Exception for splits=" + splits, e);
      }
    } else {
      final ExecutorService executor = Executors.newFixedThreadPool(splits);
      int splitSize = valueIncrements.size() / splits;
      for (int i = 0 ; i < splits ; i++) {
        executor.submit(new UpdateJob(
            incCounters, valueIncrements, maxima, i*splitSize,
            i < splits-1 ? splitSize : valueIncrements.size()-i*splitSize, statHolder));
      }
      executor.shutdown();
      try {
        executor.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Unorderly termination while waiting for " + splits + " update threads", e);
      }
    }
    return System.nanoTime()-start;
  }
  private static class UpdateJob implements Callable<UpdateJob> {
    private final Incrementable counters;
    private final PackedInts.Reader increments;
    private final PackedInts.Reader maxima;
    private final int start;
    private final int length;
    private final StatHolder statHolder;

    private UpdateJob(Incrementable counters, PackedInts.Reader increments, PackedInts.Reader maxima,
                      int start, int length, StatHolder statHolder) {
      this.counters = counters;
      this.increments = increments;
      this.maxima = maxima;
      this.start = start;
      this.length = length;
      this.statHolder = statHolder;
    }

    @Override
    public UpdateJob call() throws Exception {
      int nextPing = start + statHolder.getPingFrequency() - 1;
      for (int i = start; i < start + length; i++) {
        try {
          counters.increment((int) increments.get(i));
          if (i == nextPing) {
            statHolder.ping(statHolder.getPingFrequency());
            nextPing += statHolder.getPingFrequency();
          }
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
        int left = nextPing - statHolder.getPingFrequency() - (start + length);
        if (left > 0) {
          statHolder.ping(left);
        }
      }
      return this;
    }
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
    private PackedInts.Reader maxima;
    private final PackedInts.Mutable impl;
    private PackedInts.Reader increments;
    private final String designation;
    private final char id;

    private int splits = 1; // Suggested number of parallel updaters when running

    // [run][block] where block = {0..measurePoints}, each point being totalUpdatesPerRun/measurePoints increments
    private final List<List<Long>> timings = Collections.synchronizedList(new ArrayList<List<Long>>());
    private final int measurePoints;
    private final int totalUpdatesPerRun;

    final int incrementsPerBlock;
    public final AtomicLong lastAddNS = new AtomicLong(-1);
    public long pingsSinceLastAdd = 0;
    private final int pingFrequency = 10000; // Every ping is synchronized, so don't do this too often

    public StatHolder(
        PackedInts.Mutable impl, char id, String designation, int totalUpdatesPerRun, int measurePoints, int splits) {
      this.impl = impl;
      this.id = id;
      this.designation = designation;

      this.totalUpdatesPerRun = totalUpdatesPerRun;
      this.measurePoints = measurePoints;
      incrementsPerBlock = Math.max(1, totalUpdatesPerRun / measurePoints);
      this.splits = splits;
      System.out.println("Created StatHolder " + id + ": " + impl.getClass().getSimpleName() + ": " + designation
          + " (" + impl.ramBytesUsed()/M + "MB)" + heap());
    }

    public synchronized void ping(long updatesSinceLastPing) { // Call this ~1000 increments
      pingsSinceLastAdd += updatesSinceLastPing;
      if (pingsSinceLastAdd < incrementsPerBlock) {
        return;
      }
      long now = System.nanoTime();
      timings.get(timings.size()-1).add(now-lastAddNS.getAndSet(now));
      pingsSinceLastAdd -= incrementsPerBlock;
    }

    public double getUpdatesPerMS(int block, int entry) {
      List<Long> slice = new ArrayList<>(timings.size());
      for (List<Long> timing: timings) {
        slice.add(timing.get(block));
      }

      Collections.sort(slice);
      Collections.reverse(timings);
      final double incrementsPerBlock = 1.0*totalUpdatesPerRun/measurePoints;
      return timings.isEmpty() ? 0 : incrementsPerBlock/slice.get(entry)*MI;
    }

    public double getLastUpdatesPerMS(int block) {
      final double incrementsPerBlock = 1.0*totalUpdatesPerRun/measurePoints;
      return timings.isEmpty() ? 0 : incrementsPerBlock / timings.get(timings.size()-1).get(block)*MI;
    }

    public String toString() {
      return String.format("%-22s (%4dMB): %6d updates/ms median, %6d updates/ms max",
          id + ": " + designation, impl.ramBytesUsed()/M,
          (long)getUpdatesPerMS(getBlockCount()-1, timings.size()/2),
          (long)getUpdatesPerMS(getBlockCount()-1, timings.size()-1));
    }

    public int getBlockCount() {
      int min = Integer.MAX_VALUE;
      for (List<Long> timing: timings) {
        min = Math.min(min, timing.size());
      }
      return min == Integer.MAX_VALUE ? 0 : min;
    }

    // In preparation of testing
    public void prepareTest(PackedInts.Reader increments, PackedInts.Reader maxima) {
/*      PackedInts.Mutable clone = PackedInts.getMutable(
          increments.size(), increments.getBitsPerValue(), PackedInts.DEFAULT);
      for (int i = 0 ; i < increments.size() ; i++) {
        clone.set(i, increments.get(i));
      }
      this.increments = clone;*/
      pingsSinceLastAdd = 0;
      lastAddNS.set(System.nanoTime());
      impl.clear();
      this.increments = increments; // clone takes too much memory to be feasible
      this.maxima = maxima;
      timings.add(Collections.synchronizedList(new ArrayList<Long>(measurePoints)));
    }

    @Override
    public StatHolder call() throws Exception {
      pingsSinceLastAdd = 0;
      lastAddNS.set(System.nanoTime());
      measure(impl, increments, maxima, splits, this);
//      System.out.print(String.format(Locale.ENGLISH, "%c:%5d  ",
//          id, (long) (((double) updatesPerTiming) / ns * 1000000)));
      return this;
    }

    public void setSplits(int splits) {
      this.splits = splits;
    }

    public int getPingFrequency() {
      return pingFrequency;
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
    return new IntMutable(
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
          "-r x:  Number of runs per test case. Default: 5\n" +
          "-e x:  Which measurement to report, as an index in slowest to fastest run. Default: runs/2\n" +
          "-t x:  Number of Threads used per run for parallel tests. Default: Unlimited\n" +
          "-s x*: Split update space into this number of parts for threaded updating. Default: 1\n" +
          "-i x:  Duplicate all instances this number of times. Default: 1\n" +
          "-u x:  Number of million updates per run. Default: 10\n" +
          "-a x:  Measure points (rows in the output). Default: 50\n" +
          "-c x*: Cache-setups for N-plane. Default: 1000 500 200 111 50 20\n" +
          "-p x*: Max planes for N-plane. Default: 64\n" +
          "-m x*: Histogram maxima. Default: 425799733 85835129 52695663...\n" +
          "-d x:  Histogram multiplication factor. Default: 1.0\n\n" +
          "Note the absence of a warmup round. As the median over all runs is used, " +
          "the initial fluctuations of the JIT and the caches should not be irrelevant.";
}

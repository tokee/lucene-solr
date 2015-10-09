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
 * array-based equivalent. Candidates so far are {@link org.apache.lucene.search.HitQueueArray} and
 * {@link org.apache.lucene.search.HitQueuePacked}. Note that the focus for now is performance. As such, the new
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
public class TestHitQueue extends LuceneTestCase {
  private static final int K = 1000;
  private static final int M = K*K;

  private enum PQTYPE {Sentinel, No_Sentinel, Array, Packed, BHeap2, BHeap3, BHeap4, BHeap5}

  public void testPQArray() throws ExecutionException, InterruptedException {
    final int RUNS = 10;
    final int SKIPS= 3;
    final int THREADS = 1;

    System.out.println("Threads     pqSize   inserts  arrayMS  inserts/MS  initMS  emptyMS");
    for (int pqSize: Arrays.asList(K, 10*K, 100*K, M, 10*M, 100*M)) {
      for (int inserts : Arrays.asList(100*K, 100*M)) {//, M, 10*M)) {
        Result tArray = testPerformance(RUNS, SKIPS, THREADS, pqSize, inserts, PQTYPE.Array, random().nextLong());
        System.out.println(String.format("%7d %10d %9d %8d %11d %7d %8d",
            THREADS, pqSize, inserts,
            tArray.total()/tArray.runs/M,
            1L*inserts*tArray.runs*M/tArray.total(),
            tArray.init/tArray.runs/M,
            tArray.empty/tArray.runs/M
            ));

        // Try to avoid that heap garbage spills over to next test
        System.gc();
        Thread.sleep(100);
      }
    }

    /*
Trial run on an i7 laptop:

Threads     pqSize   inserts  arrayMS  inserts/MS  initMS  emptyMS
      1       1000    100000        4       20493       0        0
      1       1000 100000000     2528       39554       0        0
      1      10000    100000        8       11847       0        1
      1      10000 100000000     2645       37805       0        1
      1     100000    100000       30        3226       0       28
      1     100000 100000000     2725       36691       0       17
      1    1000000    100000       33        3002       1       29
      1    1000000 100000000     4366       22900       1      263
      1   10000000    100000      780         128     749       28
      1   10000000 100000000    18645        5363       9     3944
      1  100000000    100000     2212          45    2183       26
      1  100000000 100000000    91104        1097    1915    86691

     */

  }

  /**
   * In theory the impact of random memory access with vanilla HitQueue should worsen when there are more threads
   * as that lowers the chance of CPU L2/3 cache hits. The array based implementations should fare better dure to
   * their high locality of data.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQManyThreads() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(2, 4, 8, 16, 32, 64);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10, 10*K);
    final List<Integer> INSERTS = Arrays.asList(M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * See the performance behaviour of array vs. vanilla sentinel for larger queue sizes.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQArrayUpscale() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(4);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10*K, 100*K, M, 2*M);
    final List<Integer> INSERTS = Arrays.asList(100*K, M, 2*M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }


  /**
   * Test to see if the atomic array based queues are any good for top-X, where X is small (10).
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQPerformanceTinyTop() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(2);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.Sentinel, // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel  // Sanity check. Ideally this should be the same as the first Sentinel
    );
    final List<Integer> PQSIZES = Arrays.asList(10, 100);
    final List<Integer> INSERTS = Arrays.asList(10, 100, K, 10 * K, 100 * K, M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * Focus on packed vs. vanilla.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQPerformancePacked() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(4);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(K, 10, K, 10 * K, 100 * K, M);
    final List<Integer> INSERTS = Arrays.asList(10 * K, 100 * K, M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * Excessive hammering of very small queues and inserts.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQPerformanceTinySizeTinyInserts() throws ExecutionException, InterruptedException {
    final int RUNS = 500;
    final int SKIPS= 20;
    final List<Integer> threads = Arrays.asList(1, 4, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(2, 10, 20, 100);
    final List<Integer> INSERTS = Arrays.asList(0, 1, 10, 20, 100);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * Explorative test thar runs a number of combinations.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQPerformanceMulti() throws ExecutionException, InterruptedException {
    final int RUNS = 100;
    final int SKIPS= 20;
    final List<Integer> threads = Arrays.asList(1, 4, 8, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5,
//        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5,
//        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10, 100, K, 10 * K, 100 * K, M);
    final List<Integer> INSERTS = Arrays.asList(10, 100, 10 * K, 100 * K, M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  public void testPQPerformanceReport1M() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(1, 4, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5,
//        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5,
//        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(M);
    final List<Integer> INSERTS = Arrays.asList(10, 100, K, 10*K, 100*K, M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  public void testPQPerformanceAtomicHeaps() throws ExecutionException, InterruptedException {
    final int RUNS = 15;
    final int SKIPS= 3;
    final List<Integer> threads = Arrays.asList(1, 4, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Packed,  // First in list is used as base
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5,
        PQTYPE.Packed,
        PQTYPE.BHeap2,
        PQTYPE.BHeap3,
        PQTYPE.BHeap4,
        PQTYPE.BHeap5
    );
    final List<Integer> PQSIZES = Arrays.asList(M);
    final List<Integer> INSERTS = Arrays.asList(100*K, M, 2*M, 5*M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  public void testPQPerformanceReport30K() throws ExecutionException, InterruptedException {
    final int RUNS = 50;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(1);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap3,
//        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
        PQTYPE.BHeap3,
//        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(30*K);
    final List<Integer> INSERTS = Arrays.asList(10, 100, K, 10*K, 100*K, M, 10*M, 100*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  public void testPQPerformanceReport10() throws ExecutionException, InterruptedException {
    final int RUNS = 50;
    final int SKIPS= 10;
    final List<Integer> threads = Arrays.asList(1, 4, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sentinel, // First in list is used as base
        PQTYPE.No_Sentinel,
//        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sentinel,  // Sanity check. Ideally this should be the same as the first Sentinel
        PQTYPE.No_Sentinel,
//        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10);
    final List<Integer> INSERTS = Arrays.asList(10, 100, K, 10*K, 100*K, M, 10*M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  private enum COLLAPSE {none, fastest, slowest}
  private void doPerformanceTest(int runs, int skips, List<Integer> threadss, List<PQTYPE> pqTypes,
                                 List<Integer> pqSizes, List<Integer> insertss, COLLAPSE collapse)
      throws ExecutionException, InterruptedException {
    System.out.print("Threads       Top-X      Hits");
    for (PQTYPE pqType: collapse(pqTypes, collapse)) {
      System.out.print(String.format(Locale.ENGLISH, "  %12s ", pqType));
    }
    System.out.println("");

    for (int threads: threadss) {
      for (int pqSize : pqSizes) {
        for (int inserts : insertss) {
          List<Result> results = new ArrayList<>();
          long seed = random().nextLong();
          for (PQTYPE pqType : pqTypes) {
            results.add(testPerformance(runs, skips, threads, pqSize, inserts, pqType, seed));
            System.gc();
            Thread.sleep(100);
          }

          results = collapseResults(results, collapse);
          System.out.print(String.format(Locale.ENGLISH, "%7d %11d %9d", threads, pqSize, inserts));
          for (Result result : results) {
            double frac = 1D * result.total() / results.get(0).total();
            System.out.print(String.format(Locale.ENGLISH, "%9.2f %3.0f%%%1s",
                1D * result.total() / result.runs / M, frac * 100, markFastest(results, result)));
          }
          System.out.println();
        }
      }
    }
  }

  private List<PQTYPE> collapse(List<PQTYPE> pqTypes, COLLAPSE collapse) {
    return collapse == COLLAPSE.none ? pqTypes : new ArrayList<>(new LinkedHashSet<>(pqTypes)); // Order is important
  }
  private List<Result> collapseResults(List<Result> results, COLLAPSE collapse) {
    if (collapse == COLLAPSE.none) {
      return results;
    }
    List<Result> collapsed = new ArrayList<>();
    outer:
    for (Result result: results) {
      for (int i = 0 ; i < collapsed.size() ; i++) {
        Result old = collapsed.get(i);
        if (old.pqType == result.pqType) {
          switch (collapse) {
            case fastest:
              if (old.total() > result.total()) {
                collapsed.set(i, result);
              }
              break;
            case slowest:
              if (old.total() < result.total()) {
                collapsed.set(i, result);
              }
              break;
            case none: throw new IllegalStateException("Looped inner collapse should never hit case 'none'");

            default: throw new IllegalStateException(
                "Looped collapsing with method '" + collapse + "' is not supported");
          }
          continue outer;
        }
      }
      collapsed.add(result);
    }
    return collapsed;
  }

  private String markFastest(List<Result> results, Result result) {
    for (Result candidate: results) {
      if (candidate.total() < result.total()) {
        return " ";
      }
    }
    return "■";
  }

  private Result testPerformance(int runs, int skips, int threads, int pqSize, int inserts, PQTYPE pqType, long seed)
      throws ExecutionException, InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    Random random = new Random(seed);

    List<Future<Updater>> futures = new ArrayList<>(threads);
    for (int thread = 0 ; thread < threads ; thread++) {
      Updater updater = new Updater(pqSize, inserts, runs, skips, pqType, random.nextLong());
      futures.add(executor.submit(updater));
    }
    executor.shutdown();
    executor.awaitTermination(1000, TimeUnit.SECONDS);
    assertTrue("The executor should exit with all Futures processed", executor.awaitTermination(100, TimeUnit.SECONDS));

    Result total = Result.zeroSources(pqType);
    for (Future<Updater> future: futures) {
      total.add(future.get().result);
    }
/*    System.out.println(String.format(Locale.ENGLISH, "%7d %10d %9d %8b %7d %10d %9d ",
        threads, pqSize, inserts, prePopulate,
        total.init/total.runs/M,
        1L*inserts*total.runs*M/total.fill,
        total.empty/total.runs/M
    ));*/
    return total;
  }

  private static class Result {
    public long init = 0;
    public long fill = 0;
    public long empty = 0;

    public int sources = 1;
    public int runs = 0;
    public final PQTYPE pqType;

    private Result(PQTYPE pqType) {
      this.pqType = pqType;
    }

    public void add(Result other) {
      init += other.init;
      fill += other.fill;
      empty += other.empty;

      sources += other.sources;
      runs += other.runs;
    }

    public long total() {
      return init + fill + empty;
    }

    public static Result zeroSources(PQTYPE pqType) {
      Result result = new Result(pqType);
      result.sources = 0;
      return result;
    }
  }

  private class Updater implements Callable<Updater> {
    public final Result result;
    private final int inserts;
    private final int runs;
    private final int skips;
    private final Random random;
    private final int pqSize;
    private final PQTYPE pqType;

    public Updater(int pqSize, int inserts, int runs, int skips, PQTYPE pqType, long seed) {
      this.pqSize = pqSize;
      this.inserts = inserts;
      this.runs = runs;
      this.skips = skips;
      this.pqType = pqType;
      result = new Result(pqType);
      random = new Random(seed); // Local Randoms seeded up-front ensures reproducible runs
    }

    @Override
    public Updater call() throws Exception {
      final float[] scores = new float[inserts];

      for (int run = 0 ; run < runs ; run++) {
        if (run == skips) {
          result.init = 0;
          result.fill = 0;
          result.empty = 0;
          result.runs = 0;
        }

        // Random is relatively slow, so we pre-fill an array with test data before measuring time
        // Calling random in-loop increases fill time 10x for top-10 tests
        for (int i = 0 ; i < inserts ; i++) {
          scores[i] = random.nextFloat()*Float.MAX_VALUE;
        }

        result.init -= System.nanoTime();
        final PQMutant pq = new PQMutant(pqSize, pqType);
        result.init += System.nanoTime();

        result.fill -= System.nanoTime();
        for (int i = 0 ; i < inserts ; i++) {
          pq.insert(i, scores[i]); // We do not randomize docID to save test heap, but it is secondary sort key anyway
          //pq.insert(random.nextInt(Integer.MAX_VALUE), random.nextFloat()*Float.MAX_VALUE);
        }
        result.fill += System.nanoTime();

        result.empty -= System.nanoTime();
        int popCount = 0;
        Iterator<ScoreDoc> it = pq.getFlushingIterator(false, true);
        while (it.hasNext() && popCount++ < inserts) {
          if (it.next() == null) {
            break;
          }
        }
        result.empty += System.nanoTime();

        result.runs++;
      }
      return this;
    }
  }

  private class PQMutant implements HitQueueInterface {
    private final HitQueueInterface hq;

    private PQMutant(int size, PQTYPE pqType) {
      hq = createQueue(size, pqType);
    }
    private HitQueueInterface createQueue(int size, PQTYPE pqType) {
      switch (pqType) {
        case Sentinel:    return new HitQueue(size, true);
        case No_Sentinel: return new HitQueue(size, false);
        case Array:       return new HitQueueArray(size);
        case Packed:      return new HitQueuePacked(size);
        case BHeap2:      return new HitQueuePackedBHeap(size, 2);
        case BHeap3:      return new HitQueuePackedBHeap(size, 3);
        case BHeap4:      return new HitQueuePackedBHeap(size, 4);
        case BHeap5:      return new HitQueuePackedBHeap(size, 5);
        default:
          throw new IllegalStateException("Unknown PQTYPE: " + pqType);
      }
    }

    @Override
    public int size() {
      return hq.size();
    }

    @Override
    public int capacity() {
      return hq.capacity();
    }

    // Use atomic arguments based insert instead
    @Deprecated
    @Override
    public ScoreDoc insert(ScoreDoc element) {
      return hq.insert(element);
    }

    @Override
    public void insert(int docID, float score) {
      hq.insert(docID, score);
    }

    @Override
    public void clear() {
      hq.clear();
    }

    @Override
    public boolean isEmpty() {
      return hq.isEmpty();
    }

    @Override
    public Iterator<ScoreDoc> getFlushingIterator(boolean unordered, boolean reuseScoreDoc) {
      return hq.getFlushingIterator(unordered, reuseScoreDoc);
    }
  }

  // TODO: Test same score, different docIDs

  /**
   * Quick & dirty valifity test of implementations by comparing them to vanilla hitQueue.
   */
  public void testValidityMisc() {
    final int RUNS = 10;
    final int[] SIZES = new int[]{1, 2, 1000};
    final int[] INSERTS = new int[]{0, 1, 99, 1000, 1001, 10000};
    final Random random = new Random(12); // We fix for now to locate errors

    testValidity(RUNS, SIZES, INSERTS, random, PQTYPE.values());
  }

  public void testValidityProblem() {
    final int RUNS = 1;
    final int[] SIZES = new int[]{1};
    final int[] INSERTS = new int[]{2};
    final Random random = new Random(87); // We fix for now to locate errors
    final PQTYPE[] PQTYPES = new PQTYPE[]{PQTYPE.Packed};

    testValidity(RUNS, SIZES, INSERTS, random, PQTYPES);
  }

  private void testValidity(int runs, int[] sizes, int[] insertss, Random random, PQTYPE[] pqTypes) {
    for (int run = 0 ; run < runs; run++) {
      for (PQTYPE pqType: pqTypes) {
        for (int size: sizes) {
          for (int inserts: insertss) {
            PQMutant expected = new PQMutant(size, PQTYPE.Sentinel); // Maybe use clean HitQueue instead of wrapped?
            PQMutant actual = new PQMutant(size, pqType);

            for (int i = 0 ; i < inserts ; i++) {
              int doc = random.nextInt(Integer.MAX_VALUE);
              float score = random.nextFloat()*Float.MAX_VALUE;
              expected.insert(doc, score);
              actual.insert(doc, score);
            }
            assertEquals("run=" + run + ", PQType=" + pqType + ", size=" + size + ", inserts=" + inserts,
                expected, actual);
          }
        }
      }
    }
  }

  public void testBasicQueueOperationsPacked() {
    testBasicQueueOperations(new HitQueuePacked(10));
  }
  public void testBasicQueueOperationsMutant() {
    testBasicQueueOperations(new PQMutant(10, PQTYPE.Packed));
  }
  private void testBasicQueueOperations(HitQueueInterface queue) {
    String des = queue.getClass().getSimpleName();
    assertEquals("Extraction of elements from empty queue " + des + " should yield nothing", 0, asList(queue).size());
    // TODO: Test depletion by iterator -> clear
    queue.clear(); // Just to be sure

    queue.insert(new ScoreDoc(1, 2.0f));
    assertEquals("Size should be 1 after a single insert in " + des, 1, queue.size());
    ScoreDoc extracted = asList(queue).get(0);
    assertEquals("The extracted document should have the right ID for " + des,
        1, extracted.doc);
    assertTrue("The extracted document from " + des + " should have score 2.0f but was " + extracted.score,
        Math.abs(2.0f - extracted.score) < 0.0001);
  }
  public void testPackedFull() {
    {
      HitQueueInterface packed = new HitQueuePacked(3);
      packed.insert(new ScoreDoc(3, 3.0f));
      packed.insert(new ScoreDoc(1, 1.0f));
      packed.insert(new ScoreDoc(2, 2.0f));
      List<ScoreDoc> extracted = asList(packed);
      String all = "";
      for (ScoreDoc scoreDoc: extracted) {
        all += "\n" + scoreDoc;
      }
      assertEquals("The number of extracted elements for just filled should match", 3, extracted.size());
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(1, 1.0f), extracted.get(0));
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(2, 2.0f), extracted.get(1));
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(3, 3.0f), extracted.get(2));
    }
    {
      HitQueueInterface packed = new HitQueuePacked(3);
      packed.insert(new ScoreDoc(3, 3.0f));
      packed.insert(new ScoreDoc(1, 1.0f));
      packed.insert(new ScoreDoc(2, 2.0f));
      packed.insert(new ScoreDoc(4, 4.0f));
      List<ScoreDoc> extracted = asList(packed);
      assertEquals("The number of extracted elements for ever filled should match", 3, extracted.size());
      String all = "";
      for (ScoreDoc scoreDoc: extracted) {
        all += "\n" + scoreDoc;
      }
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(2, 2.0f), extracted.get(0));
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(3, 3.0f), extracted.get(1));
      assertScoreDocEquals("Just filled" + all, new ScoreDoc(4, 4.0f), extracted.get(2));
    }
  }

  public void testEqualScore() {
    {
      HitQueueInterface vanilla = new PQMutant(3, PQTYPE.Sentinel);
      vanilla.insert(new ScoreDoc(3, 3.0f));
      vanilla.insert(new ScoreDoc(1, 3.0f));
      vanilla.insert(new ScoreDoc(2, 3.0f));

      HitQueueInterface packed = new HitQueuePacked(3);
      packed.insert(new ScoreDoc(3, 3.0f));
      packed.insert(new ScoreDoc(1, 3.0f));
      packed.insert(new ScoreDoc(2, 3.0f));

      assertEquals("Equal score, different docIDs should work", vanilla, packed);
    }
  }
  public void testPackedOne() {
    HitQueueInterface packed = new HitQueuePacked(1);
    packed.insert(new ScoreDoc(1, 1.0f));
    List<ScoreDoc> extracted = asList(packed);
    assertEquals("The number of extracted elements from a filled size 1 queue should be 1", 1, extracted.size());
    assertScoreDocEquals("Single scoreDoc", new ScoreDoc(1, 1.0f), extracted.get(0));
  }

  public void testPackedOneExtra() {
    HitQueueInterface packed = new HitQueuePacked(1);
    packed.insert(new ScoreDoc(1, 1.0f));
    packed.insert(new ScoreDoc(3, 3.0f));
    packed.insert(new ScoreDoc(2, 2.0f));
    List<ScoreDoc> extracted = asList(packed);
    assertEquals("The number of extracted elements from an over filled size 1 queue should be 1", 1, extracted.size());
    assertScoreDocEquals("Single scoreDoc", new ScoreDoc(3, 3.0f), extracted.get(0));
  }

  public void testPackedOneExtraSpecific() {
    final Float f1 = 1.5726789E38f;
    final int doc1 = 1905463594;
    final Float f2 = 1.8786446E38f;
    final int doc2 = 1559930263;
    HitQueueInterface packed = new PQMutant(1, PQTYPE.Sentinel);
    packed.insert(new ScoreDoc(doc2, f2));
    packed.insert(new ScoreDoc(doc1, f1));

    List<ScoreDoc> extracted = asList(packed);
    assertEquals("The number of extracted elements from an over filled size 1 queue should be 1", 1, extracted.size());
    assertScoreDocEquals("Specific single scoreDoc", new ScoreDoc(doc2, f2), extracted.get(0));
  }

  public void testPackedZero() {
    HitQueueInterface packed = new HitQueuePacked(0);
    List<ScoreDoc> extracted = asList(packed);
    assertEquals("The number of extracted elements from a zero size queue should be 0", 0, extracted.size());
  }

  private void assertScoreDocEquals(String message, ScoreDoc e, ScoreDoc a) {
    String debug =
        "expected(score=" + e.score + ", docID=" + e.doc + "), actual(score=" + a.score + ", docID=" + a.doc + ")";
    assertTrue(message + ". The scores should be equal: " + debug,
        Math.abs(e.score - a.score) < 0.000001); // Semi-arbitrary low limit
    assertEquals(message + ". The docIDs should be equal: " + debug,
        e.doc, a.doc);
  }

  private void assertEquals(String message, HitQueueInterface expected, HitQueueInterface actual) {
    // Queue size not reliable due to sentinel objects in vanilla HitQueue
//    assertEquals(message + ". Sizes should be equal", expected.size(), actual.size());
    List<ScoreDoc> expectedList = asList(expected);
    List<ScoreDoc> actualList = asList(actual);
    assertEquals(message + ". Extracted lists of equal size", expectedList.size(), actualList.size());
    for (int i = 0 ; i < expectedList.size() ; i++) {
      ScoreDoc e = expectedList.get(i);
      ScoreDoc a = actualList.get(i);
      String debug =
          "expected(score=" + e.score + ", docID=" + e.doc + "), actual(score=" + a.score + ", docID=" + a.doc + ")";
      for (int j = Math.max(0, i-2) ; j < Math.min(expectedList.size(), i+3) ; j++) {
        debug += "\n" + j + ": " + actualList.get(j);
        if (j == i) {
          debug += " *";
        }
      }
      assertFalse(message + ". The actual score at index " + i + " should never be NaN: " + debug,
          Float.isNaN(a.score));
      assertFalse(message + ". The actual score at index " + i + " should never be < 0: " + debug,
          a.score < 0f);
      assertTrue(message + ". The scores for index=" + i + " should be equal: " + debug,
          Math.abs(e.score - a.score) < 0.000001); // Semi-arbitrary low limit
      assertEquals(message + ". The docIDs for index=" + i + " should be equal: " + debug,
          e.doc, a.doc);
    }
  }

  private List<ScoreDoc> asList(HitQueueInterface queue) {
    List<ScoreDoc> list = new ArrayList<>(queue.size());
    Iterator<ScoreDoc> it = queue.getFlushingIterator(false, false);
    while (it.hasNext()) {
      list.add(it.next());
    }
    return list;
  }



  /* Sanity check of the theory that the binary representation of positive floats is ordered the same way as the
     abstract float. This ordering is a prerequisite for HitQueuePacked.
   */
  public void testFloatBits() {
    final int RUNS = 100000;
    for (int i = 0 ; i < RUNS ; i++) {
      float f1 = random().nextFloat();
      float f2 = random().nextFloat();
      int fb1 = Float.floatToRawIntBits(f1);
      assertTrue("Converting the float " + f1 + " to bits and back should be reflective. " +
          "Expected " + f1 + ", got " + Float.intBitsToFloat(fb1),
          Math.abs(f1 - Float.intBitsToFloat(fb1)) < 0.00001);
      int fb2 = Float.floatToRawIntBits(f2);
      if (f1 > f2) {
        assertTrue("Float " + f1 + " > " + f2 + ", so binary values should also match: " + fb1 + " > " + fb2,
            fb1 > fb2);
      } else if (f1 < f2) {
        assertTrue("Float " + f1 + " < " + f2 + ", so binary values should also match: " + fb1 + " < " + fb2,
            fb1 < fb2);
      } // We ignore equals as we do not use NaN and other special floats

    }
  }
}

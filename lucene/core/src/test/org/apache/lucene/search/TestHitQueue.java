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
 * - Sent:   The default Solr HitQueue with sentinel objects, used for standard Solr top-X searches.
 * - NoSent: Same as Sent, but without sentinel objects.
 * - Array:  Instead of storing the heap as an array of Objects, two atomic arrays (one for scores, one for docIDs)
 *           are used.
 * - Packed: Instead of storing the heap as an array of Objects, a single atomic array of longs is used, where score
 *           and docID is packed together.
 */
public class TestHitQueue extends LuceneTestCase {
  private static final int K = 1000;
  private static final int M = K*K;

  private enum PQTYPE {Sent, NoSent, Array, Packed}

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
        PQTYPE.Sent, // First in list is used as base
        PQTYPE.NoSent,
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sent,  // Sanity check. Ideally this should be the same as the first Sent
        PQTYPE.NoSent,
        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10, 10*K);
    final List<Integer> INSERTS = Arrays.asList(M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * See the performance behaviour of array vs. vanilla sentinel as the queue size grows.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQArrayScale() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(2);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sent, // First in list is used as base
        PQTYPE.Packed,
        PQTYPE.Sent,  // Sanity check. Ideally this should be the same as the first Sent
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10, 100, K, 10*K, 100*K, M, 2*M);
    final List<Integer> INSERTS = Arrays.asList(100*K, M, 3*M);

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
        PQTYPE.Sent, // First in list is used as base
        PQTYPE.NoSent,
        PQTYPE.Sent, // Sanity check. Ideally this should be the same as the first Sent
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sent  // Sanity check. Ideally this should be the same as the first Sent
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
        PQTYPE.Sent, // First in list is used as base
        PQTYPE.Packed,
        PQTYPE.Sent,  // Sanity check. Ideally this should be the same as the first Sent
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(K, 10, K, 10 * K, 100 * K, M);
    final List<Integer> INSERTS = Arrays.asList(10 * K, 100 * K, M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  /**
   * Explorative test thar runs a number of combinations.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void testPQPerformanceMulti() throws ExecutionException, InterruptedException {
    final int RUNS = 50;
    final int SKIPS= 5;
    final List<Integer> threads = Arrays.asList(1, 4, 16);
    final List<PQTYPE> pqTypes = Arrays.asList(
        PQTYPE.Sent, // First in list is used as base
        PQTYPE.NoSent,
        PQTYPE.Array,
        PQTYPE.Packed,
        PQTYPE.Sent,  // Sanity check. Ideally this should be the same as the first Sent
        PQTYPE.NoSent,
        PQTYPE.Array,
        PQTYPE.Packed
    );
    final List<Integer> PQSIZES = Arrays.asList(10, K, 10 * K, 100 * K, M);
    final List<Integer> INSERTS = Arrays.asList(100, 10 * K, 100 * K, M);

    doPerformanceTest(RUNS, SKIPS, threads, pqTypes, PQSIZES, INSERTS, COLLAPSE.fastest);
  }

  private enum COLLAPSE {none, fastest, slowest}
  private void doPerformanceTest(int runs, int skips, List<Integer> threadss, List<PQTYPE> pqTypes,
                                 List<Integer> pqSizes, List<Integer> insertss, COLLAPSE collapse)
      throws ExecutionException, InterruptedException {
    System.out.print("Threads     pqSize   inserts");
    for (PQTYPE pqType: collapse(pqTypes, collapse)) {
      System.out.print(String.format("%7s_ms/%% ", pqType));
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
          System.out.print(String.format("%7d %10d %9d", threads, pqSize, inserts));
          for (Result result : results) {
            double frac = 1D * result.total() / results.get(0).total();
            System.out.print(String.format("%6d %5.1f%1s",
                result.total() / result.runs / M, frac * 100, markFastest(results, result)));
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
/*    System.out.println(String.format("%7d %10d %9d %8b %7d %10d %9d ",
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

      for (int run = 0 ; run < runs ; run++) {
        if (run == skips) {
          result.init = 0;
          result.fill = 0;
          result.empty = 0;
          result.runs = 0;
        }

        result.init -= System.nanoTime();
        final PQMutant pq = new PQMutant(pqSize, pqType);
        result.init += System.nanoTime();

        result.fill -= System.nanoTime();
        ScoreDoc scoreDoc = pq.getInitial();
        for (int i = 0 ; i < inserts ; i++) {
          if (scoreDoc == null) {
            scoreDoc = new ScoreDoc(0, 0.0f);
          }
          scoreDoc.doc = random.nextInt();
          scoreDoc.score = random.nextFloat();
          scoreDoc = pq.insert(scoreDoc);
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
    private final PQTYPE pqType;
    private final HitQueueInterface hq;

    private PQMutant(int size, PQTYPE pqType) {
      this.pqType = pqType;
      switch (pqType) {
        case Sent:
          hq = new HitQueue(size, true);
          break;
        case NoSent:
          hq = new HitQueue(size, false);
          break;
        case Array:
          hq = new HitQueueArray(size);
          break;
        case Packed:
          hq = new HitQueuePacked(size);
          break;
        default:
          throw new IllegalStateException("Unknown PQTYPE: " + pqType);
      }
    }

    public ScoreDoc getInitial() {
      return pqType == PQTYPE.Sent ? ((HitQueue)hq).top() : new ScoreDoc(0, 0f);
    }

    @Override
    public int size() {
      return hq.size();
    }

    @Override
    public int capacity() {
      return hq.capacity();
    }

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

  public void testFloatBits() {
    final int RUNS = 100000;
    for (int i = 0 ; i < RUNS ; i++) {
      float f1 = random().nextFloat();
      float f2 = random().nextFloat();
      int fb1 = Float.floatToRawIntBits(f1);
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

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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PriorityQueue;

public class TestHitQueue extends LuceneTestCase {
  private static final int K = 1000;
  private static final int M = K*K;

  public void testPQArray() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final int threads = 4;
    System.out.println("Threads     pqSize   inserts  arrayMS");
    for (int pqSize: Arrays.asList(K, 10*K, 100*K, M, 10*M, 100*M)) {
      for (int inserts : Arrays.asList(100*K, M, 10*M)) {
        Result tArray = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, false, false);
        System.out.println(String.format("%7d %10d %9d %8d",
            threads, pqSize, inserts,
            tArray.total()/tArray.runs/1000000
            ));
      }
    }

  }

  public void testPQPerformanceSpecific() throws ExecutionException, InterruptedException {
    final int RUNS = 20;
    final int SKIPS= 5;
    final int threads = 4;

    System.out.println("Threads     pqSize   inserts  trueMS  falseMS  arrayMS   falseFrac  arrayFrac  arrayFracF");
    for (int pqSize: Arrays.asList(10, K, 10*K, 100*K, M)) {
      for (int inserts: Arrays.asList(10, K, 10*K, 100*K, M)) {
        Result tFalse = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, false, true);
        // Try to avoid that heap garbage spills over to next test
        System.gc();
        Thread.sleep(100);

        Result tTrue = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, true, true);
        System.gc();
        Thread.sleep(100);

        Result tArray = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, false, false);
        System.gc();
        Thread.sleep(100);

        double falseGain = 1D*tFalse.total()/tTrue.total();
        double arrayGain = 1D*tArray.total()/tTrue.total();
        double arrayGainF = 1D*tArray.total()/tFalse.total();
        System.out.println(String.format("%7d %10d %9d "
            + "%7d %8d %8d  %9.2f%% %9.2f%%  %9.2f%%",
            threads, pqSize, inserts,
            tTrue.total()/tTrue.runs/1000000,
            tFalse.total()/tFalse.runs/1000000,
            tArray.total()/tArray.runs/1000000,
            falseGain*100,
            arrayGain*100,
            arrayGainF*100
            ));
        // Try to avoid that heap garbage spills over to next test
        System.gc();
        Thread.sleep(50);
      }
    }
  }

  private Result testPerformance(
      int runs, int skips, int threads, int pqSize, int inserts, boolean prePopulate, boolean vanilla)
      throws ExecutionException, InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threads);

    List<Future<Updater>> futures = new ArrayList<>(threads);
    for (int thread = 0 ; thread < threads ; thread++) {
      Updater updater = new Updater(pqSize, inserts, runs, skips, prePopulate, vanilla);
      futures.add(executor.submit(updater));
    }
    executor.shutdown();
    executor.awaitTermination(1000, TimeUnit.SECONDS);
    assertTrue("The executor should exit with all Futures processed", executor.awaitTermination(100, TimeUnit.SECONDS));

    Result total = Result.zeroSources();
    for (Future<Updater> future: futures) {
      total.add(future.get().result);
    }
/*    System.out.println(String.format("%7d %10d %9d %8b %7d %10d %9d ",
        threads, pqSize, inserts, prePopulate,
        total.init/total.runs/1000000,
        1L*inserts*total.runs*1000000/total.fill,
        total.empty/total.runs/1000000
    ));*/
    return total;
  }

  private static class Result {
    public long init = 0;
    public long fill = 0;
    public long empty = 0;

    public int sources = 1;
    public int runs = 0;

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

    public static Result zeroSources() {
      Result result = new Result();
      result.sources = 0;
      return result;
    }
  }

  private class Updater implements Callable<Updater> {
    public final Result result = new Result();
    private final int inserts;
    private final int runs;
    private final int skips;
    private final Random random;
    private final int pqSize;
    private final boolean prePopulate;
    private final boolean vanilla;

    public volatile int sync;

    public Updater(int pqSize, int inserts, int runs, int skips, boolean prePopulate, boolean vanilla) {
      this.pqSize = pqSize;
      this.inserts = inserts;
      this.runs = runs;
      this.skips = skips;
      this.prePopulate = prePopulate;
      this.vanilla = vanilla;
      random = new Random(random().nextLong()); // Local Randoms seeded up-front ensures reproducible runs
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
        final PQMutant pq = new PQMutant(pqSize, prePopulate, vanilla);
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
        while (pq.size() > 0 && popCount++ < inserts) {
          if (pq.pop(scoreDoc) == null) {
            break;
          }
        }
        result.empty += System.nanoTime();

        result.runs++;
      }
      return this;
    }
  }

  private class PQMutant {
    private final boolean prePopulate;
    private final boolean vanilla;

    private final HitQueue hqVanilla;
    private final HitQueueArray hqArray;

    private PQMutant(int size, boolean prePopulate, boolean vanilla) {
      this.prePopulate = prePopulate;
      this.vanilla = vanilla;

      hqVanilla = vanilla ? new HitQueue(size, prePopulate) : null;
      hqArray = vanilla ? null : new HitQueueArray(size);
    }

    public ScoreDoc getInitial() {
      return vanilla && prePopulate ? hqVanilla.top() : new ScoreDoc(0, 0f);
    }

    public ScoreDoc insert(ScoreDoc scoreDoc) {
      if (vanilla) {
        if (prePopulate) {
          hqVanilla.updateTop();
          return hqVanilla.top();
        } else {
          return hqVanilla.insertWithOverflow(scoreDoc);
        }
      }
      return hqArray.insertWithOverflow(scoreDoc);
    }

    public int size() {
      return vanilla ? hqVanilla.size() : hqArray.size();
    }

    public ScoreDoc pop(ScoreDoc reuse) {
      return vanilla ? hqVanilla.pop() : hqArray.pop(reuse);
    }
  }

}

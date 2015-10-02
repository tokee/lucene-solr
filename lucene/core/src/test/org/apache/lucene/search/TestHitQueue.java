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

  public void testPQPerformanceDiverse() throws ExecutionException, InterruptedException {
    final int RUNS = 100;
    final int SKIPS= 10;

    System.out.println("Threads     pqSize   inserts  prepPop  initMS inserts/ms   emptyMS");
    for (int threads: Arrays.asList(1, 2, 4, 8, 16, 32, 64)) {
      for (int pqSize: Arrays.asList(10, 100, 1000, 10000)) {
        for (int inserts: Arrays.asList(10, 100, 1000, 10000)) {
          for (Boolean prePopulate: Arrays.asList(true, false)) {
            Result total = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, prePopulate);
            System.out.println(String.format("%7d %10d %9d %8b %7d %10d %9d ",
                threads, pqSize, inserts, prePopulate,
                total.init/total.runs/1000000,
                1L*inserts*total.runs*1000000/total.fill,
                total.empty/total.runs/1000000
            ));
          }
        }
      }
    }
    System.out.println("Finished testing");
  }

  public void testPQPerformanceSpecific() throws ExecutionException, InterruptedException {
    final int RUNS = 100;
    final int SKIPS= 10;
    final int threads = 4;

    System.out.println("Threads     pqSize   inserts  trueMS  falseMS  falseTime");
    for (int pqSize: Arrays.asList(10, 1000, 100000, 1000000)) {
      for (int inserts: Arrays.asList(10, 1000, 100000, 1000000)) {
        Result tFalse = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, false);
        // Try to avoid that heap garbage spills over to next test
        System.gc();
        Thread.sleep(100);

        Result tTrue = testPerformance(RUNS, SKIPS, threads, pqSize, inserts, true);
        System.gc();
        Thread.sleep(100);

        double falseGain = 1D*tFalse.total()/tTrue.total();
                System.out.println(String.format("%7d %10d %9d "
                    + "%7d %8d %9.2f%%",
                    threads, pqSize, inserts,
                    tTrue.total()/tTrue.runs/1000000,
                    tFalse.total()/tFalse.runs/1000000,
                    falseGain*100
                ));
        // Try to avoid that heap garbage spills over to next test
        System.gc();
        Thread.sleep(50);
      }
    }
  }

  private Result testPerformance(int runs, int skips, int threads, int pqSize, int inserts, boolean prePopulate)
      throws ExecutionException, InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threads);

    List<Future<Updater>> futures = new ArrayList<>(threads);
    for (int thread = 0 ; thread < threads ; thread++) {
      Updater updater = new Updater(pqSize, inserts, runs, skips, prePopulate);
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

    public volatile int sync;

    public Updater(int pqSize, int inserts, int runs, int skips, boolean prePopulate) {
      this.pqSize = pqSize;
      this.inserts = inserts;
      this.runs = runs;
      this.skips = skips;
      this.prePopulate = prePopulate;
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
        final PriorityQueue<ScoreDoc> pq = new HitQueue(pqSize, prePopulate);
        result.init += System.nanoTime();

        result.fill -= System.nanoTime();
        ScoreDoc scoreDoc = prePopulate ? pq.top() : null;
        for (int i = 0 ; i < inserts ; i++) {
          if (scoreDoc == null) {
            scoreDoc = new ScoreDoc(0, 0.0f);
          }
          scoreDoc.doc = random.nextInt();
          scoreDoc.score = random.nextFloat();
          if (prePopulate) {
            pq.updateTop();
          } else {
            scoreDoc = pq.insertWithOverflow(scoreDoc);
          }
        }
        result.fill += System.nanoTime();

        result.empty -= System.nanoTime();
        int popCount = 0;
        while (pq.size() > 0 && popCount++ < inserts) {
          if (pq.pop() == null) {
            break;
          }
        }
        result.empty += System.nanoTime();

        result.runs++;
      }
      return this;
    }
  }

}

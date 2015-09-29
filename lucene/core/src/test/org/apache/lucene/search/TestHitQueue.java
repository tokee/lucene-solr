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

  public void testPQPerformance() throws ExecutionException, InterruptedException {
    final int RUNS = 100;

    System.out.println("Threads     pqSize   inserts  prepPop  initMS inserts/ms  empty/ms");
    for (int threads: Arrays.asList(1, 2, 4, 8, 16, 32, 64)) {
      for (int pqSize: Arrays.asList(10, 100, 1000, 10000)) {
        for (int inserts: Arrays.asList(10, 100, 1000, 10000)) {
          for (Boolean prePopulate: Arrays.asList(true, false)) {
            testPerformance(RUNS, threads, pqSize, inserts, prePopulate);
          }
        }
      }
    }
  }

  private void testPerformance(int runs, int threads, int pqSize, int inserts, boolean prePopulate)
      throws ExecutionException, InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threads);

    List<Future<Updater>> futures = new ArrayList<>(threads);
    for (int thread = 0 ; thread < threads ; thread++) {
      Updater updater = new Updater(pqSize, inserts, runs, prePopulate);
      futures.add(executor.submit(updater));
      executor.submit(updater);
    }
    executor.shutdown();
    assertTrue("The executor should exit with all Futures processed",        executor.awaitTermination(100, TimeUnit.SECONDS));

    long totalInitTime = 0L;
    long totalFillTime = 0L;
    long totalEmptyTime = 0L;
    for (Future<Updater> future: futures) {
      totalInitTime += future.get().initTime;
      totalFillTime += future.get().fillTime;
      totalEmptyTime += future.get().emptyTime;
    }
    System.out.println(String.format("%7d %10d %9d %8b %7d %10d %9d ",
        threads, pqSize, inserts, prePopulate,
        totalInitTime/threads/runs/1000000,
        1L*inserts*threads*runs*1000000/totalFillTime,
        1L*threads*runs*1000000/totalEmptyTime)
    );
  }



  private class Updater implements Callable<Updater> {
    public long initTime = 0;  // nano
    public long fillTime = 0;  // nano
    public long emptyTime = 0; // nano
    private final int inserts;
    private final int runs;
    private final Random random;
    private final int pqSize;
    private final boolean prePopulate;

    public volatile int sync;

    public Updater(int pqSize, int inserts, int runs, boolean prePopulate) {
      this.pqSize = pqSize;
      this.inserts = inserts;
      this.runs = runs;
      this.prePopulate = prePopulate;
      random = new Random(random().nextLong()); // Local Randoms seeded up-front ensures reproducible runs
    }

    @Override
    public Updater call() throws Exception {

      for (int run = 0 ; run < runs ; run++) {
        initTime -= System.nanoTime();
        final PriorityQueue<ScoreDoc> pq = new HitQueue(pqSize, prePopulate);
        initTime += System.nanoTime();

        fillTime -= System.nanoTime();
        ScoreDoc scoreDoc = null;
        for (int i = 0 ; i < inserts ; i++) {
          if (scoreDoc == null) {
            scoreDoc = new ScoreDoc(0, 0.0f);
          }
          scoreDoc.doc = random.nextInt();
          scoreDoc.score = random.nextFloat();
          scoreDoc = pq.insertWithOverflow(scoreDoc);
        }
        fillTime += System.nanoTime();

        emptyTime -= System.nanoTime();
        while (pq.size() > 0) {
          if (pq.pop() == null) {
            break;
          }
        }
        emptyTime += System.nanoTime();

      }
      sync = 87; // Attempt to ensure that times are flushed trrough the caches
      return this;
    }
  }

}

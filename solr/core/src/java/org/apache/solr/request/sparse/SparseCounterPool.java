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
package org.apache.solr.request.sparse;

import org.apache.lucene.util.packed.PackedInts;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Maintains a pool SparseCounters, taking care of allocation, used counter clearing and re-use.
 * </p><p>
 * The pool and/or the content of the pool is bound to the index. When the index is updated and a new facet
 * request is issued, the pool will be cleared.
 * </p><p>
 * Constantly allocating large counter structures taxes the garbage collector of the JVM, so it is faster to
 * clear & re-use structures instead. This is especially true for sparse structures, where the clearing time
 * is proportional to the number of previously updated counters.
 * </p><p>
 * The pool has a fixed maximum limit. If there is not cleaned counters available when requested, a new one
 * will be created. Ig the pool has reached maximum size and a counter is released, the counter is de-referenced,
 * removing it from the heap. It is advisable to set the pool size to a bit more than the number of concurrent
 * faceted searches, to avoid new allocations.
 * </p><p>
 * Setting the pool size very high has the adverse effect of a constant heap overhead, dictated by the maximum
 * number of concurrent facet requests encountered since last index (re)open.
 * </p><p>
 * Default behaviour for the pool is to perform counter clearing in 1 background thread, as this makes it possible
 * to provide a facet result to the caller faster. This can be controlled with {@link #setCleaningThreads(int)}.
 * </p><p>
 * This class is thread safe and with no heavy synchronized parts.
 */
@SuppressWarnings("NullableProblems")
public class SparseCounterPool {
  // Number of threads for background cleaning. If this is 0, cleans will be performed directly in {@link #release}.
  public static final int DEFAULT_CLEANING_THREADS = 1;

  private int max;
  private final List<ValueCounter> pool;
  private String key = null;

  // Pool stats
  private long reuses = 0;
  private long allocations = 0;
  private long packedAllocations = 0;
  private long lastMaxCountForAny = 0;
  private long clears = 0;
  private long frees = 0;

  // Counter stats
  long sparseCalls = 0;
  long skipCount = 0;
  String lastSkipReason = "no skips";
  long sparseAllocateTime = 0;
  long sparseCollectTime = 0;
  long sparseExtractTime = 0;
  long sparseClearTime = 0;
  long sparseTotalTime = 0;
  long disables = 0;
  long withinCutoffCount = 0;
  long exceededCutoffCount = 0;

  String lastTermsLookup = "N/A";
  long termsCountsLookups = 0;
  long termsFallbackLookups = 0;
  String lastTermLookup = "N/A";
  long termCountsLookups = 0;
  long termFallbackLookups = 0;

  protected final ThreadPoolExecutor cleaner =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(DEFAULT_CLEANING_THREADS);
  {
    cleaner.setThreadFactory(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("SparsePoolCleaner");
        return t;
      }
    });
    Runtime.getRuntime().addShutdownHook(new Thread() { // Play nice with shutdown
      @Override
      public void run() {
        cleaner.shutdown();
      }
    });
  }


  private final Object scSync = new Object();

  public SparseCounterPool(int maxPoolSize) {
    this.max = maxPoolSize;
    pool =  new ArrayList<ValueCounter>();
  }

  /**
   * Delivers a counter ready for updates.
   * </p><p>
   * Note: This assumes that the maximum count for any counter it Integer.MAX_VALUE (2^31).
   * If the maximum value is know, it is highly recommended to use {@link #acquire(int, long, SparseKeys)} instead
   * as that makes it possible to deliver optimized counters.
   * @param counts     the number of entries in the counter.
   * @param sparseKeys generic setup for the Sparse system.
   * @return a counter ready for updates.
   */
  public ValueCounter acquire(int counts, SparseKeys sparseKeys) {
    return acquire(counts, Integer.MAX_VALUE, sparseKeys);
  }

  /**
   * Delivers a counter ready for updates. The type of counter will be chosen based on counts, maxCountForAny and
   * the general Sparse setup from sparseKeys. This is the recommended way to get sparse counters.
   * @param counts     the number of entries in the counter.
   * @param maxCountForAny the maximum value that any individual counter can reach.
   * @param sparseKeys generic setup for the Sparse system.
   * @return a counter ready for updates.
   */
  public ValueCounter acquire(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    final long allocateTime = System.nanoTime();
    if (maxCountForAny == 0 && sparseKeys.packed) {
      throw new IllegalStateException("Attempted to request sparse counter with maxCountForAny=" + maxCountForAny);
    }
    try {
      String acquireKey = getKey(counts, maxCountForAny, sparseKeys);
      synchronized (this) {
        if (key != null && key.equals(acquireKey) && !pool.isEmpty()) {
          reuses++;
          return pool.remove(pool.size()-1);
        }
      }
      return getCounter(counts, maxCountForAny, sparseKeys);
    } finally {
      incAllocateTime(System.nanoTime() - allocateTime);
    }
  }

  private String getKey(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    return usePacked(counts, maxCountForAny, sparseKeys) ?
        SparseCounterPacked.getID(
            counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked) :
        SparseCounterInt.getID(counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction);
  }

  private boolean usePacked(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    return (sparseKeys.packed && PackedInts.bitsRequired(maxCountForAny) <= sparseKeys.packedLimit) ||
            maxCountForAny > Integer.MAX_VALUE;
  }

  private ValueCounter getCounter(int counts, long maxCountForAny, SparseKeys sparseKeys) {
    synchronized (this) {
      allocations++;
      lastMaxCountForAny = maxCountForAny;
    }
    if (usePacked(counts, maxCountForAny, sparseKeys)) {
      synchronized (this) {
        packedAllocations++;
      }
      return new SparseCounterPacked(
          counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
    }
    return new SparseCounterInt(counts, maxCountForAny, sparseKeys.minTags, sparseKeys.fraction, sparseKeys.maxCountsTracked);
  }


  /**
   * Release a counter after use. The counter should not be cleared before release as this is handled by
   * the SparseCounterPool itself.
   * </p><p>
   * If {@link #getCleaningThreads} is >= 1, the cleaning will be performed in the background and the method
   * will return immediately. If there are 0 cleaning threads, the cleaning will be done up front and the method
   * will return after it has finished. Cleaning time is proportional to the number of updated counters.
   * @param counter a used counter.
   */
  public void release(ValueCounter counter) {
    // Initial fast sanity check and potential pool cleaning
    synchronized (this) {
      if (counter.explicitlyDisabled()) {
        disables++;
      }
      if (key != null && !counter.getKey().equals(key)) {
        // Setup changed, clear all!
        pool.clear();
        key = null;
      }
      if (pool.size() + cleaner.getQueue().size() >= max) {
        // Pool full and/or too many counters in clean queue; just release the counter to GC
        frees++;
        return;
      }
      clears++; // We now know a clear will be performed
    }

    if (cleaner.getCorePoolSize() == 0) {
      // No background cleaning, so we must do it up front
      long clearTime = System.nanoTime();
      counter.clear();
      incClearTime(System.nanoTime() - clearTime);
      releaseCleared(counter);
    } else {
      // Send the cleaning to the background thread(s)
      cleaner.execute(new FutureTask<ValueCounter>( new BackgroundClear(counter)));
    }
  }

  /**
   * Called by the background cleaner when a counter has been processed.
   * @param counter a fully cleaned counter, ready for use.
   */
  private void releaseCleared(ValueCounter counter) {
    synchronized (this) {
      if (key != null && !counter.getKey().equals(key) || pool.size() >= max) {
        // Setup changed or pool full. Skip insert!
        frees++;
        return;
      }
      pool.add(counter);
      key = counter.getKey();
    }
  }

  /**
   * @return the maximum amount of counters in this pool.
   */
  public int getMax() {
    return max;
  }

  /**
   * @param max the maximum amount of counters in this pool.
   */
  public void setMax(int max) {
    synchronized (this) {
      if (this.max == max) {
        return;
      }
      while (pool.size() > max) {
        pool.remove(pool.size()-1);
      }
      this.max = max;
    }
  }

  /**
   * 0 cleaning threads means that cleaning will be performed upon {@link #release(ValueCounter)}.
   * @return the amount of Threads used for cleaning counters in the background.
   */
  public int getCleaningThreads() {
    return cleaner.getCorePoolSize();
  }

  /**
   * 0 cleaning threads means that cleaning will be performed upon {@link #release(ValueCounter)}.
   * @param threads the amount of threads used for cleaning counters in the background.
   */
  public void setCleaningThreads(int threads) {
    synchronized (cleaner) {
      cleaner.setCorePoolSize(threads);
    }
  }

  /**
   * Clears both the pool and any accumulated statistics.
   */
  public void clear() {
    synchronized (this) {
      pool.clear();
      // TODO: Find a clean way to remove non-started tasks from the cleaner
      key = "discardResultsFromBackgroundClears";
      reuses = 0;
      allocations = 0;
      packedAllocations = 0;
      lastMaxCountForAny = 0;
      clears = 0;
      frees = 0;

      sparseCalls = 0;
      skipCount = 0;
      sparseAllocateTime = 0;
      sparseCollectTime = 0;
      sparseExtractTime = 0;
      sparseClearTime = 0;
      sparseTotalTime = 0;
      withinCutoffCount = 0;
      disables = 0;
      exceededCutoffCount = 0;
      lastSkipReason = "no skips";

      lastTermsLookup = "N/A";
      termsCountsLookups = 0;
      termsFallbackLookups = 0;
      lastTermLookup = "N/A";
      termCountsLookups = 0;
      termFallbackLookups = 0;
    }
  }

  public void incSparseCalls() {
    synchronized (scSync) {
      sparseCalls++;
    }
  }
  public void incSkipCount(String reason) {
    synchronized (scSync) {
      skipCount++;
    }
    lastSkipReason = reason;
  }
  public void incWithinCount() {
    synchronized (scSync) {
      withinCutoffCount++;
    }
  }
  public void incExceededCount() {
    synchronized (scSync) {
      exceededCutoffCount++;
    }
  }
  private void incAllocateTime(long delta) {
    synchronized (scSync) {
      sparseAllocateTime += delta;
    }
  }
  public void incCollectTime(long delta) {
    synchronized (scSync) {
      sparseCollectTime += delta;
    }
  }
  public void incExtractTime(long delta) {
    synchronized (scSync) {
      sparseExtractTime += delta;
    }
  }
  private void incClearTime(long delta) {
    synchronized (scSync) {
      sparseClearTime += delta;
    }
  }
  public void incTotalTime(long delta) {
    synchronized (scSync) {
      sparseTotalTime += delta;
    }
  }
  public void incTermsLookup(String terms, boolean countsStructure) {
    lastTermsLookup = terms;
    if (countsStructure) {
      termsCountsLookups++;
    } else {
      termsFallbackLookups++;
    }
  }
  public void incTermLookup(String term, boolean countsStructure) {
    lastTermLookup = term;
    if (countsStructure) {
      termCountsLookups++;
    } else {
      termFallbackLookups++;
    }
  }

  /**
   * @return timing and count statistics for this pool.
   */
  @Override
  public String toString() {
    if (sparseCalls == 0) {
      return "No sparse faceting performed yet";
    }
    final int M = 1000000;
    synchronized (this) { // We need to synchronize this to get accurate counts for the pool
      final int pendingCleans = cleaner.getQueue().size() + cleaner.getActiveCount();
      final int poolSize = pool.size();
      final int cleanerCoreSize = cleaner.getCorePoolSize();
      return String.format(
          "sparse statistics: calls=%d, fallbacks=%d (last: %s), collect=%dms avg, extract=%dms avg, " +
              "total=%dms avg, disables=%d,  withinCutoff=%d, exceededCutoff=%d, SCPool(cached=%d/%d, reuses=%d, " +
              "allocations=%d (%dms avg, %d packed), clears=%d (%dms avg," +
              " %s%s), " +
              "frees=%d, lastMaxCountForAny=%d), terms(count=%d, fallback=%d, last#=%d)," +
              "term(count=%d, fallback=%d, last=%s)",
          sparseCalls, skipCount, lastSkipReason, sparseCollectTime/sparseCalls/M, sparseExtractTime/sparseCalls/M,
          sparseTotalTime/sparseCalls/M, disables, withinCutoffCount, exceededCutoffCount, poolSize, max, reuses,
          allocations, sparseAllocateTime/sparseCalls/M, packedAllocations, clears, sparseClearTime /sparseCalls/M,
          cleanerCoreSize > 0 ? "background" : "at release",
          pendingCleans > 0 ? (" (" + pendingCleans + " running)") : "",
          frees, lastMaxCountForAny, termsCountsLookups, termsFallbackLookups, lastTermsLookup.split(",").length,
          termCountsLookups, termFallbackLookups, lastTermLookup);
    }
  }

  /**
   * Clears the given counter and calls {@link #releaseCleared(ValueCounter)} when done.
      */
  private class BackgroundClear implements Callable<ValueCounter> {
    private final ValueCounter counter;

    private BackgroundClear(ValueCounter counter) {
      this.counter = counter;
    }

    @Override
    public ValueCounter call() throws Exception {
      final long startTime = System.nanoTime();
      counter.clear();
      incClearTime(System.nanoTime() - startTime);
      releaseCleared(counter);
      return counter;
    }
  }
}

package org.apache.solr.search.sparse.cache;

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

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.sparse.SparseKeys;
import org.apache.solr.search.sparse.track.ValueCounter;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of all SparseCounterPools used by the Sparse faceting system.
 * </p><p>
 * This class is thread safe and with no heavy synchronized parts.
 */
@SuppressWarnings("NullableProblems")
public class SparseCounterPoolController implements SolrCache<String, SparseCounterPool> {

  public static final String CACHE_NAME = "SparseCounterPoolController";

  private final AtomicInteger max;
  private final Map<String, SparseCounterPool> pools;
  // Shared between all SparseCounterPools
  private final ThreadPoolExecutor janitorSupervisor;

  private State state = State.CREATED;

  public SparseCounterPoolController(final int maxPoolCount, final int cleanupThreads) {
    this.max = new AtomicInteger(maxPoolCount);
    janitorSupervisor = (ThreadPoolExecutor)Executors.newFixedThreadPool(cleanupThreads);
    janitorSupervisor.setThreadFactory(new ThreadFactory() {
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
        janitorSupervisor.shutdown();
      }
    });

    pools = new LinkedHashMap<String, SparseCounterPool>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, SparseCounterPool> eldest) {
        return size() > max.get();
      }
    };
  }

  /**
   * Acquires a pool for the given field.
   * @param field       the field for the pool.
   * @param maxPoolSize the maximum size of the pool for the field.
   * @return a pool of {@link ValueCounter}s.
   */
  public SparseCounterPool acquire(String field, String description, int maxPoolSize, int minEmptyCounters) {
    SparseCounterPool pool = pools.get(field);
    if (pool == null) {
      pool = new SparseCounterPool(janitorSupervisor, field, description, maxPoolSize, minEmptyCounters);
      pools.put(field, pool);
    } else {
      pool.setMaxPoolSize(maxPoolSize);
      pool.setMinEmptyCounters(minEmptyCounters);
      pools.put(field, pool); // Updates the FIFO-queue
    }
    return pool;
  }

  /**
   * Acquires a pool for the given field if it exists in the pool controller.
   * @param field       the field for the pool.
   * @return a pool of {@link ValueCounter}s if present in the controller.
   */
  public SparseCounterPool acquireIfExisting(String field) {
    return pools.get(field);
  }

  /**
   * Clears all pools.
   */
  @Override
  public void clear() {
    pools.clear();
  }

  public int getMaxPoolCount() {
    return max.get();
  }

  public void setMaxPoolCount(int max) {
    if (max < this.max.get()) {
      pools.clear(); // TODO: Make a cleaner reduce
    }
    this.max.set(max);
  }

  /** SolrCache implementation */

//  @Override
//  public String getVersion() {
//    return "1.0";
//  }

  @Override
  public String getDescription() {
    return "Constructs and caches SparseCounterPools used by the sparse faceting code";
  }

  @Override
  public Category getCategory() {
    return Category.CACHE;
  }

//  @Override
//  public String getSource() {
//    return "$URL$";
//  }

//  @Override
//  public URL[] getDocs() {
//    try {
//      return new URL[]{
//          new URL("http://tokee.github.io/lucene-solr/")
//      };
//    } catch (MalformedURLException e) {
//      return new URL[0];
//    }
//  }

//  @Override
//  public NamedList getStatistics() {
//    return new NamedList<>(pools);
//  }

  @Override
  public String getName() {
    return CACHE_NAME;
  }

  @Override
  public String name() {
    return CACHE_NAME;
  }

  @Override
  public int size() {
    return pools.size();
  }

  @Override
  public SparseCounterPool put(String key, SparseCounterPool value) {
    return null;
  }

  @Override
  public SparseCounterPool get(String key) {
    return pools.get(key);
  }

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    if (args.containsKey(SparseKeys.POOL_MAX_COUNT)) {
      max.set(Integer.parseInt((String) args.get(SparseKeys.POOL_MAX_COUNT)));
    }
    return null; // Ignore persistence and regenerator for now
  }

  @Override
  public void setState(State state) {
    this.state = state;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<String, SparseCounterPool> old) {
    // Ignored for now
  }

  @Override
  public void close() {
    clear();
  }

  // TODO: Re-implement all metric-code below with sane behaviour instead of just "get it to compile"
  private MetricRegistry registry;
  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String scope) {
    registry = manager.registry(registryName);
    
    manager.registerGauge(this, registryName, () -> "sparse", true, "facetType", Category.CACHE.toString(), scope);
  }

  private Set<String> metricNames = new HashSet<>();
  @Override
  public Set<String> getMetricNames() {
    return null;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

}

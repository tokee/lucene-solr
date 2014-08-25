package org.apache.solr.request.sparse;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Keeps track of all SparseCounterPools used by the Sparse faceting system.
 * </p><p>
 * This class is thread safe and with no heavy synchronized parts.
 */
@SuppressWarnings("NullableProblems")
public class SparseCounterPoolController {

  private int max;
  private final Map<String, SparseCounterPool> pools;

  public SparseCounterPoolController(final int maxPoolCount) {
    this.max = maxPoolCount;
    pools = new LinkedHashMap<String, SparseCounterPool>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, SparseCounterPool> eldest) {
        return size() > max;
      }
    };
  }

  /**
   * Acquires a pool for the given field.
   * @param field       the field for the pool.
   * @param maxPoolSize the maximum size of the pool for the field.
   * @return a pool of {@link ValueCounter}s.
   */
  public SparseCounterPool acquire(String field, int maxPoolSize) {
    SparseCounterPool pool = pools.get(field);
    if (pool == null) {
      pool = new SparseCounterPool(maxPoolSize);
      pools.put(field, pool);
    } else {
      pool.setMax(maxPoolSize);
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
  public void clear() {
    pools.clear();
  }

  public int getMaxPoolCount() {
    return max;
  }

  public void setMaxPoolCount(int max) {
    if (max < this.max) {
      pools.clear(); // TODO: Make a cleaner reduce
    }
    this.max = max;
  }
}

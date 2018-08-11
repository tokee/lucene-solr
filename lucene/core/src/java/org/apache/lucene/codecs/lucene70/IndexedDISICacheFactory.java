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
package org.apache.lucene.codecs.lucene70;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Creates and stores caches for {@link IndexedDISI}.
 *
 * See https://issues.apache.org/jira/browse/LUCENE-8374 for details
 */
// Note: This was hacked together with little understanding of overall caching principles for Lucene.
// It probably belongs somewhere else and hopefully someone with a better understanding will refactor the code.
public class IndexedDISICacheFactory implements Accountable {
  public static int MIN_LENGTH_FOR_CACHING = 50; // Set this very low: Could be 9 EMPTY followed by a SPARSE
  public static boolean BLOCK_CACHING_ENABLED = true;
  public static boolean DENSE_CACHING_ENABLED = true;

  static boolean DEBUG = true; // TODO (Toke): Remove this when code has stabilized

  // Map<IndexInput.hashCode, Map<key, cache>>
  private static final Map<Integer, Map<Long, IndexedDISICache>> pool = new HashMap<>();

  static {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() +
          ": LUCENE-8374 beta patch enabled with block_caching=" + BLOCK_CACHING_ENABLED +
          ", dense_caching=" + DENSE_CACHING_ENABLED);
    }
  }

  /**
   * Releases all caches associated with the given data.
   * @param data with {@link IndexedDISICache}s.
   */
  public static void release(IndexInput data) {
    if (pool.remove(data.hashCode()) != null) {
      debug("Release cache called for data " + data.hashCode() +
          " with existing cache");
    }
  }

  /**
   * Creates a cache if not already present and returns it.
   * @param data   the slice to create a cache for.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param cost same af the cost that will also be used for creating an {@link IndexedDISI}.
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public static IndexedDISICache getCache(IndexInput data, long offset, long length, long cost, String name) throws IOException {
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    Map<Long, IndexedDISICache> caches = pool.computeIfAbsent(data.hashCode(), poolHash -> new HashMap<>());
    long key = data.hashCode() + offset + length + cost;
    IndexedDISICache cache = caches.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(data.slice("docs", offset, length),
          BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, name);
      caches.put(key, cache);
      debug("Created cache for " + data.toString() + ": " + cache.creationStats + " (" + cache.ramBytesUsed() + " bytes)");
    }
    return cache;
  }

  /**
   * Creates a cache if not already present and returns it.
   * @param poolHash the key for the map of caches in the {@link #pool}.
   * @param slice    the input slice.
   * @param cost     same af the cost that will also be used for creating an {@link IndexedDISI}.
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public static IndexedDISICache getCache(int poolHash, IndexInput slice, long cost, String name) throws IOException {
    final long offset = slice.getFilePointer();
    final long length = slice.length();
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }
    final long cacheHash = poolHash + offset + length + cost;

    Map<Long, IndexedDISICache> caches = pool.computeIfAbsent(poolHash, key -> new HashMap<>());

    IndexedDISICache cache = caches.get(cacheHash);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(slice, BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, name);
      caches.put(cacheHash, cache);
      debug("Created cache for " + slice.toString() + ": " + cache.creationStats + " (" + cache.ramBytesUsed() + " bytes)");
    }
    return cache;
  }

  // TODO (Toke): Definitely not the way to do it. Connect to InputStream or just remove it fully when IndexedDISICache is stable
  public static void debug(String message) {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() + ": " + message);
    }
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.shallowSizeOf(pool);
    for (Map.Entry<Integer, Map<Long, IndexedDISICache>> entry: pool.entrySet()) {
      mem += RamUsageEstimator.shallowSizeOf(entry);
      for (Map.Entry<Long, IndexedDISICache> cacheEntry: entry.getValue().entrySet()) {
        mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
        mem += cacheEntry.getValue().ramBytesUsed();
      }
    }
    return mem;
  }

}

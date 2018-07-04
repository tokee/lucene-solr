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

/* $Id:$
 *
 * WordWar.
 * Copyright (C) 2012 Toke Eskildsen, te@ekot.dk
 *
 * This is confidential source code. Unless an explicit written permit has been obtained,
 * distribution, compiling and all other use of this code is prohibited.
 */
package org.apache.lucene.codecs.lucene70;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;

/**
 * Creates and stores caches for {@link IndexedDISI}.
 *
 * See https://issues.apache.org/jira/browse/LUCENE-8374 for details
 */
// Note: This was hacked together with little understanding of overall caching principles for Lucene.
// It probably belongs somewhere else and hopefully someone with a better understanding will refactor the code.
public class IndexedDISICacheFactory {
  public static int MIN_LENGTH_FOR_CACHING = 50; // Set this very low: Could be 9 EMPTY followed by a SPARSE
  public static boolean BLOCK_CACHING_ENABLED = true;
  public static boolean DENSE_CACHING_ENABLED = false; // Not functioning yet

  private static boolean DEBUG = false; // TODO: Remove this when code has stabilized

  // Map<IndexInput.hashCode, Map<key, cache>>
  private static final Map<Integer, Map<Long, IndexedDISICache>> pool = new HashMap<>();

  /**
   * Releases all caches associated with the given data.
   * @param data with {@link IndexedDISICache}s.
   */
  public static void release(IndexInput data) {
    debug("Release cache called for data " + data.hashCode() + " with exists=" + pool.remove(data.hashCode()));
  }

  /**
   * Creates a cache if not already present and returns it.
   * @param data   the slice to create a cache for.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param cost same af the cost that will also be used for creating an {@link IndexedDISI}.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public static IndexedDISICache getCache(IndexInput data, long offset, long length, long cost) throws IOException {
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }
    Map<Long, IndexedDISICache> caches = pool.computeIfAbsent(data.hashCode(), key -> new HashMap<>());

    long key = data.hashCode() + offset + length + cost;
    IndexedDISICache cache = caches.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(data.slice("docs", offset, length),
          BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED);
      caches.put(key, cache);
      debug("Created cache for " + data.toString() + ": " + cache.creationStats);
    }
    return cache;
  }

  // TODO: Definitely not the way to do it. Connect to InputStream or just remove it fully when IndexedDISICache is stable
  private static void debug(String message) {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() + ": " + message);
    }
  }
}

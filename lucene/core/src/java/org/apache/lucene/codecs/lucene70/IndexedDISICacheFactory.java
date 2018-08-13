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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
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
  public static boolean VARYINGBPV_CACHING_ENABLED = true;

  public static boolean DEBUG = true; // TODO (Toke): Remove this when code has stabilized

  // Map<IndexInput.hashCode, Map<key, cache>>
  private static final Map<Integer, Map<Long, IndexedDISICache>> disiPool = new HashMap<>();
  private static final Map<Integer, Map<String, VaryingBPVJumpTable>> vBPVPool = new HashMap<>();

  static {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() +
          ": LUCENE-8374 beta patch enabled with block_caching=" + BLOCK_CACHING_ENABLED +
          ", dense_caching=" + DENSE_CACHING_ENABLED + ", cBPV_caching=" + VARYINGBPV_CACHING_ENABLED);
    }
  }

  /**
   * Releases all caches associated with the given data.
   * @param data with {@link IndexedDISICache}s.
   */
  public static void release(IndexInput data) {
    if (disiPool.remove(data.hashCode()) != null) {
      //debug("Release cache called for disiPool data " + data.hashCode() + " with existing cache");
    }
    if (vBPVPool.remove(data.hashCode()) != null) {
      //debug("Release cache called for vBPVPool data " + data.hashCode() + " with existing cache");
    }
  }

  /**
   * Creates a cache (jump table) if not already present and returns it.
   * @param indexInputHash hash for the outer IndexInput. Used for cache invalidation.
   * @param name the name for the cache, typically the field name. Used as key for later retrieval.
   * @param slice the long values with varying bits per value.
   * @param valuesLength the length in bytes of the slice.
   * @return a jump table for the longs in the given slice or null if the structure is not suitable for caching.
   */
  public static VaryingBPVJumpTable getVBPVJumpTable(
      int indexInputHash, String name, RandomAccessInput slice, long valuesLength) throws IOException {
    Map<String, VaryingBPVJumpTable> jumpTables = vBPVPool.computeIfAbsent(indexInputHash, poolHash -> new HashMap<>());
    VaryingBPVJumpTable jumpTable = jumpTables.get(name);
    if (jumpTable == null) {
      // TODO: Avoid overlapping builds of the same jump table
      jumpTable = new VaryingBPVJumpTable(slice, name, valuesLength);
      jumpTables.put(name, jumpTable);
      debug("Created packed numeric jump table for " + name + ": " + jumpTable.creationStats + " (" + jumpTable.ramBytesUsed() + " bytes)");
    }
    return jumpTable;
  }

  public static long getDISIBlocksWithOffsetsCount() {
    return disiPool.values().stream().map(Map::values).flatMap(Collection::stream).
        filter(IndexedDISICache::hasOffsets).count();
  }

  public static long getDISIBlocksWithRankCount() {
    return disiPool.values().stream().map(Map::values).flatMap(Collection::stream).
        filter(IndexedDISICache::hasRank).count();
  }

  public static long getVaryingBPVCount() {
    return vBPVPool.values().stream().map(Map::values).count();
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

    Map<Long, IndexedDISICache> caches = disiPool.computeIfAbsent(data.hashCode(), poolHash -> new HashMap<>());
    long key = data.hashCode() + offset + length + cost;
    IndexedDISICache cache = caches.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(data.slice("docs", offset, length),
          BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, name);
      caches.put(key, cache);
      debug("Created IndexedDISI cache for " + data.toString() + ": " + cache.creationStats + " (" + cache.ramBytesUsed() + " bytes)");
    }
    return cache;
  }

  /**
   * Creates a cache if not already present and returns it.
   * @param poolHash the key for the map of caches in the {@link #disiPool}.
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

    Map<Long, IndexedDISICache> caches = disiPool.computeIfAbsent(poolHash, key -> new HashMap<>());

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
    long mem = RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.shallowSizeOf(disiPool);
    for (Map.Entry<Integer, Map<Long, IndexedDISICache>> entry: disiPool.entrySet()) {
      mem += RamUsageEstimator.shallowSizeOf(entry);
      for (Map.Entry<Long, IndexedDISICache> cacheEntry: entry.getValue().entrySet()) {
        mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
        mem += cacheEntry.getValue().ramBytesUsed();
      }
    }
    for (Map.Entry<Integer, Map<String, VaryingBPVJumpTable>> entry: vBPVPool.entrySet()) {
      mem += RamUsageEstimator.shallowSizeOf(entry);
      for (Map.Entry<String, VaryingBPVJumpTable> cacheEntry: entry.getValue().entrySet()) {
        mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
        mem += cacheEntry.getValue().ramBytesUsed();
      }
    }
    return mem;
  }

  /**
   * Jump table used by Lucene70DocValuesProducer.VaryingBPVReader to avoid iterating all blocks from
   * current to wanted index. The jump table holds offsets for all blocks.
   */
  public static class VaryingBPVJumpTable implements Accountable {
    // TODO: It is much too heavy to use longs here for practically all indexes. Maybe a PackedInts representation?
    long[] offsets = new long[10];
    final String creationStats;

    public VaryingBPVJumpTable(RandomAccessInput slice, String name, long valuesLength) throws IOException {
      final long startTime = System.nanoTime();

      int block = -1;
      long offset;
      long blockEndOffset = 0;

      int bitsPerValue;
      // TODO (Toke): Introduce jump table
      do {
        offset = blockEndOffset;

        offsets = ArrayUtil.grow(offsets, block+2); // No-op if large enough
        offsets[block+1] = offset;

        bitsPerValue = slice.readByte(offset++);
        offset += Long.BYTES; // Skip over delta as we do not resolve the values themselves at this point
        if (bitsPerValue == 0) {
          blockEndOffset = offset;
        } else {
          final int length = slice.readInt(offset);
          offset += Integer.BYTES;
          blockEndOffset = offset + length;
        }
        block++;
      } while (blockEndOffset < valuesLength-Byte.BYTES-Long.BYTES);
      offsets = ArrayUtil.copyOfSubArray(offsets, 0, block+1);
      creationStats = String.format(
          "name=%s, blocks=%d, time=%dms",
          name, offsets.length, (System.nanoTime()-startTime)/1000000);
    }

    public long getBlockOffset(long block) {
      // Technically a limitation in caching vs. VaryingBPVReader to limit to 2b blocks
      return offsets[(int) block];
    }

    @Override
    public long ramBytesUsed() {
      return (offsets == null ? 0 : RamUsageEstimator.sizeOf(offsets)) +
          RamUsageEstimator.NUM_BYTES_OBJECT_REF*3 +
          RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;
    }
  }
}

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
import java.util.function.Supplier;

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
// All Lucene70NormsProducer and Lucene70DocValuesProducer instances holds their own factory, but maybe this
// should be in a central cache somewhere?
public class IndexedDISICacheFactory implements Accountable {
  public static int MIN_LENGTH_FOR_CACHING = 50; // Set this very low: Could be 9 EMPTY followed by a SPARSE

  // TODO (Toke): Remove this when code has stabilized
  public static boolean BLOCK_CACHING_ENABLED = true;
  public static boolean DENSE_CACHING_ENABLED = true;
  public static boolean VARYINGBPV_CACHING_ENABLED = true;
  public static boolean DEBUG = true;

  private final Map<Long, IndexedDISICache> disiPool = new HashMap<>();
  private final Map<String, VaryingBPVJumpTable> vBPVPool = new HashMap<>();

  static {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() +
          ": LUCENE-8374 beta patch enabled with block_caching=" + BLOCK_CACHING_ENABLED +
          ", dense_caching=" + DENSE_CACHING_ENABLED + ", cBPV_caching=" + VARYINGBPV_CACHING_ENABLED);
    }
  }

  public IndexedDISI createCachedIndexedDISI(IndexInput data, long offset, long length, long cost, String name)
      throws IOException {
    IndexedDISICache cache = getCache(data, offset, length, cost, name);
    return new IndexedDISI(data, offset, length, cost, cache, name);
    }

  public IndexedDISI createCachedIndexedDISI(IndexInput data, int cost, String name) throws IOException {
    return createCachedIndexedDISI(data, data.getFilePointer(), data.length(), cost, name);
  }

  /**
   * Creates a cache (jump table) if not already present and returns it.
   * @param name the name for the cache, typically the field name. Used as key for later retrieval.
   * @param slice the long values with varying bits per value.
   * @param valuesLength the length in bytes of the slice.
   * @return a jump table for the longs in the given slice or null if the structure is not suitable for caching.
   */
  public VaryingBPVJumpTable getVBPVJumpTable(
      String name, RandomAccessInput slice, long valuesLength) throws IOException {
    if (!VARYINGBPV_CACHING_ENABLED) {
      return null;
    }

    VaryingBPVJumpTable jumpTable = vBPVPool.get(name);
    if (jumpTable == null) {
      // TODO: Avoid overlapping builds of the same jump table
      jumpTable = new VaryingBPVJumpTable(slice, name, valuesLength);
      vBPVPool.put(name, jumpTable);
      debug("Created packed numeric jump table for " + name + ": " +
          jumpTable.creationStats + " (total " + jumpTable.ramBytesUsed() + " bytes)");
    }
    return jumpTable;
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
  public IndexedDISICache getCache(IndexInput data, long offset, long length, long cost, String name) throws IOException {
    if (!(BLOCK_CACHING_ENABLED || DENSE_CACHING_ENABLED) || length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    long key = offset + length;
    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(data.slice("docs", offset, length),
          BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, name);
      disiPool.put(key, cache);
      debug("Created IndexedDISI cache for " + data.toString() + ": " + cache.creationStats + " (" + cache.ramBytesUsed() + " bytes)");
    }
    return cache;
  }

  /**
   * Creates a cache if not already present and returns it.
   * @param slice    the input slice.
   * @param cost     same af the cost that will also be used for creating an {@link IndexedDISI}.
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public IndexedDISICache getCache(IndexInput slice, long cost, String name) throws IOException {
    final long offset = slice.getFilePointer();
    final long length = slice.length();
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    final long key = offset + length;
    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      cache = new IndexedDISICache(slice, BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, name);
      disiPool.put(key, cache);
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

  public long getDISIBlocksWithOffsetsCount() {
    return disiPool.values().stream().filter(IndexedDISICache::hasOffsets).count();
  }

  public long getDISIBlocksWithRankCount() {
    return disiPool.values().stream().filter(IndexedDISICache::hasRank).count();
  }

  public long getVaryingBPVCount() {
    return vBPVPool.size();
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this) +
        RamUsageEstimator.shallowSizeOf(disiPool) +
        RamUsageEstimator.shallowSizeOf(vBPVPool);
    for (Map.Entry<Long, IndexedDISICache> cacheEntry: disiPool.entrySet()) {
        mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
      mem += RamUsageEstimator.sizeOf(cacheEntry.getKey());
        mem += cacheEntry.getValue().ramBytesUsed();
      }
    for (Map.Entry<String, VaryingBPVJumpTable> cacheEntry: vBPVPool.entrySet()) {
      String key = cacheEntry.getKey();
        mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
      mem += RamUsageEstimator.shallowSizeOf(key)+key.length()*2;
        mem += cacheEntry.getValue().ramBytesUsed();
      }
    return mem;
  }

  /**
   * Releases all caches.
   */
  public void releaseAll() {
    disiPool.clear();
    vBPVPool.clear();
  }

  /**
   * Jump table used by Lucene70DocValuesProducer.VaryingBPVReader to avoid iterating all blocks from
   * current to wanted index. The jump table holds offsets for all blocks.
   */
  public static class VaryingBPVJumpTable implements Accountable {
    // TODO: It is way overkill to use longs here for practically all indexes. Maybe a PackedInts representation?
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
      long[] newOffsets = new long[block+1];
      System.arraycopy(offsets, 0, newOffsets, 0, block+1);
      offsets = newOffsets;
      //offsets = ArrayUtil.copyOfSubArray(offsets, 0, block+1);
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
      return RamUsageEstimator.shallowSizeOf(this) +
          (offsets == null ? 0 : RamUsageEstimator.sizeOf(offsets)) + // offsets
          RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;  // creationStats
    }
  }
}

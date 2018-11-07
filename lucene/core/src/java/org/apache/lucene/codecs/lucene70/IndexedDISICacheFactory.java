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
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Creates and stores caches for {@link IndexedDISI} and {@link Lucene70DocValuesProducer}.
 * The caches are stored in maps, where the key is made up from offset and length of a slice
 * in an underlying segment. Each segment uses their own IndexedDISICacheFactory.
 *
 * See {@link IndexedDISICache} for details on the caching.
 */
public class IndexedDISICacheFactory implements Accountable {

  /**
   * If the slice with the DISI-data is less than this number of bytes, don't create a cache.
   * This is a very low number as the DISI-structure very efficiently represents EMPTY & ALL blocks.
   */
  public static int MIN_LENGTH_FOR_CACHING = 50; // Set this very low: Could be 9 EMPTY followed by a SPARSE

  // TODO (Toke): Remove this when code has stabilized
  // The on/off switches are static as per-call switching would change a lot of logic in Lucene.
  // They are intended for experimentation and expected to be removed after LUCENE-8374 stabilization.
  public static boolean NORMS_CACHING_ENABLED = true;
  public static boolean BLOCK_CACHING_ENABLED = true;
  public static boolean DENSE_CACHING_ENABLED = true;
  public static boolean VARYINGBPV_CACHING_ENABLED = true;
  public static boolean DEBUG = true;

  // jump-table and rank for DISI blocks
  private final Map<Long, IndexedDISICache> disiPool = new HashMap<>();
  // jump-table for numerics with variable bits per value (dates, longs...)
  private final Map<String, VaryingBPVJumpTable> vBPVPool = new HashMap<>();

  /**
   * Debug-oriented setter for toggling lucene8374 caching.
   * @param switches comma-separated list of caches to enable: norms, block, dense, vbpv, debug.
   *                 Caches not mentioned in the list are disabled. Also supported are explicit flags, e.g.
   *                 "norms=false,block=true"
   * @return human readable description of what is enabled, with error message if the enable-string could not be parsed.
   */
  // TODO (Toke): Remove this when code has stabilized
  public static String setEnabled(String switches) {
    // Shortcuts
    switch (switches) {
      case "all":
      case "true":
      case "on": {
        switches = "norm,block,dense,vbpv,debug";
        break;
      }
      case "none":
      case "false":
      case "off": {
        switches = "";
        break;
      }
    }

    // Collect enabled
    Set<String> enabled = new HashSet<>(10);
    String[] tokens = switches.split(", *");
    for (String token: tokens) {
      String keyValue[] = token.split("=");
      if (keyValue.length == 1 || Boolean.parseBoolean(keyValue[1])) {
        enabled.add(keyValue[0].toLowerCase(Locale.ENGLISH));
      }
    }

    // Toggle if needed
    NORMS_CACHING_ENABLED = enabled.contains("norm") || enabled.contains("norms");
    BLOCK_CACHING_ENABLED = enabled.contains("block");
    DENSE_CACHING_ENABLED = enabled.contains("dense");
    VARYINGBPV_CACHING_ENABLED = enabled.contains("vbpv");
    DEBUG = enabled.contains("debug");;

    return getEnabled();
  }
  public static String getEnabled() {
    return String.format("lucene8374(norms=%b, block=%b, dense=%b, vBPV=%b, debug=%b)",
        NORMS_CACHING_ENABLED, BLOCK_CACHING_ENABLED, DENSE_CACHING_ENABLED, VARYINGBPV_CACHING_ENABLED, DEBUG);
  }

  // TODO (Toke): Remove this when code has stabilized
  static {
    if (DEBUG) {
      System.out.println(IndexedDISICacheFactory.class.getSimpleName() +
          ": LUCENE-8374 beta patch enabled with default caching " +
          "norms=" + NORMS_CACHING_ENABLED +
          ", block=" + BLOCK_CACHING_ENABLED +
          ", dense=" + DENSE_CACHING_ENABLED +
          ", vBPV=" + VARYINGBPV_CACHING_ENABLED);
    }
  }

  /**
   * Create a cached {@link IndexedDISI} instance.
   * @param data   persistent data containing the DISI-structure.
   * @param cost   cost as defined for IndexedDISI.
   * @param name   identifier for the DISI-structure for debug purposes.
   * @return a cached IndexedDISI or a plain IndexedDISI, if caching is not applicable.
   * @throws IOException if the DISI-structure could not be accessed.
   */
  public IndexedDISI createCachedIndexedDISI(IndexInput data, long key, int cost, String name) throws IOException {
    IndexedDISICache cache = getCache(data, key, name);
    return new IndexedDISI(data, cost, cache, name);
  }

  /**
   * Create a cached {@link IndexedDISI} instance.
   * @param data   persistent data containing the DISI-structure.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param cost   cost as defined for IndexedDISI.
   * @param name   identifier for the DISI-structure for debug purposes.
   * @return a cached IndexedDISI or a plain IndexedDISI, if caching is not applicable.
   * @throws IOException if the DISI-structure could not be accessed.
   */
  public IndexedDISI createCachedIndexedDISI(IndexInput data, long offset, long length, long cost, String name)
      throws IOException {
    IndexedDISICache cache = getCache(data, offset, length, name);
    return new IndexedDISI(data, offset, length, cost, cache, name);
  }

  /**
   * Creates a cache (jump table) for variable bits per value numerics and returns it.
   * If the cache has previously been created, the old cache is returned.
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
   * Creates a cache (jump table) for {@link IndexedDISI}.
   * If the cache has previously been created, the old cache is returned.
   * @param data   the slice to create a cache for.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public IndexedDISICache getCache(IndexInput data, long offset, long length, String name) throws IOException {
    if (!(BLOCK_CACHING_ENABLED || DENSE_CACHING_ENABLED) || length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    long key = offset + length;
    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache for performance reason
      cache = new IndexedDISICache(data.slice("docs", offset, length), true, true, name);
      disiPool.put(key, cache);
      debug("Created IndexedDISI cache for " + data.toString() + ": " + cache.creationStats + " (" + cache.ramBytesUsed() + " bytes)");
    }
    return cache;
  }

  /**
   * Creates a cache (jump table) for {@link IndexedDISI}.
   * If the cache has previously been created, the old cache is returned.
   * @param slice the input slice.
   * @param key identifier for the cache, unique within the segment that originated the slice.
   *            Recommendation is offset+length for the slice, relative to the data mapping the segment.
   *            Warning: Do not use slice.getFilePointer and slice.length as they are not guaranteed
   *            to be unique within the segment (slice.getFilePointer is 0 when a sub-slice is created).
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public IndexedDISICache getCache(IndexInput slice, long key, String name) throws IOException {
    final long length = slice.length();
    if (!(BLOCK_CACHING_ENABLED || DENSE_CACHING_ENABLED) || length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      // Both BLOCK & DENSE caches are created as they might be requested later for the field,
      // regardless of whether they are requested now
      cache = new IndexedDISICache(slice, true, true, name);
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

  // Statistics
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
   * current to wanted index for numerics with variable bits per value. The jump table holds offsets for all blocks.
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
      } while (blockEndOffset < valuesLength-Byte.BYTES);
      offsets = ArrayUtil.copyOfSubArray(offsets, 0, block+1);
      creationStats = String.format(
          "name=%s, blocks=%d, time=%dms",
          name, offsets.length, (System.nanoTime()-startTime)/1000000);
    }

    /**
     * @param block the logical block in the vBPV structure ( valueindex/16384 ).
     * @return the index slice offset for the vBPV block (1 block = 16384 values) or -1 if not available.
     */
    public long getBlockOffset(long block) {
      // Technically a limitation in caching vs. VaryingBPVReader to limit to 2b blocks
      return IndexedDISICacheFactory.VARYINGBPV_CACHING_ENABLED ? offsets[(int) block] : -1;
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.shallowSizeOf(this) +
          (offsets == null ? 0 : RamUsageEstimator.sizeOf(offsets)) + // offsets
          RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;  // creationStats
    }
  }
}

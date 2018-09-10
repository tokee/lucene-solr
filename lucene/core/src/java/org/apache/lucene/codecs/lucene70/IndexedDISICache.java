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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene70.IndexedDISI.MAX_ARRAY_LENGTH;

/**
 * Caching of IndexedDISI with two strategies:
 *
 * A lookup table for block blockCache and index and a rank structure for DENSE block lookups.
 *
 * The lookup table is an array of {@code long}s with an entry for each block (65536 bits).
 * Each long entry consists of 2 logical parts:
 * The first 31 bits holds the index up to just before the wanted block.
 * The next 33 bits holds the offset into the underlying slice.
 * As there is a maximum of 2^16 blocks, it follows that the maximum size of any block must
 * not exceed 2^17 bits to avoid  overflow. This is currently the case, with the largest
 * block being DENSE and using 2^16 + 32 bits, and is likely to continue to hold as using
 * more than double the amount of bits is unlikely to be an efficient representation.
 * The alternative to using the lookup table is iteration of all blocks up to the wanted one.
 * The cache overhead is numDocs/1024 bytes.
 *
 * Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits).
 * In the case of non-existing blocks, the entry in the lookup table has index equal to the
 * previous entry and offset equal to the next non-empty block.
 *
 * The rank structure for DENSE blocks is an array of unsigned {@code short}s with an entry
 * or each sub-block of 512 bits out of the 65536 bits in the outer block.
 * Each rank-entry states the number of set bits within the block up to the bit before the
 * bit positioned at the start of the sub-block.
 * Note that that the rank entry of the first sub-block is always 0 and that the last entry can
 * at most be 65536-512 = 65024 and thus will always fit into an unsigned short.
 * See https://en.wikipedia.org/wiki/Succinct_data_structure for details on rank structures.
 * The alternative to using the rank structure is iteration and summing of set bits for all
 * entries in the DENSE sub-block up until the wanted bit, with a worst-case of 1024 entries.
 * The rank cache overhead for a single DENSE block is 128 shorts (128*16 = 2048 bits) or
 * 1/32th.
 *
 * The total overhead for the rank cache is currently also numDocs/32 bits or numDocs/8 bytes
 * as the rank-representation is not sparse itself, using empty entries for sub-blocks of type
 * ALL or SPARSE.
 *
 * See https://issues.apache.org/jira/browse/LUCENE-8374 for details
 */
public class IndexedDISICache implements Accountable {

  public static final int BLOCK = 65536;
  public static final int BLOCK_BITS = 16;
  public static final long BLOCK_INDEX_SHIFT = 33;
  public static final long BLOCK_INDEX_MASK = ~0L << BLOCK_INDEX_SHIFT;
  public static final long BLOCK_LOOKUP_MASK = ~BLOCK_INDEX_MASK;

  public static final int RANK_BLOCK = 512;
  public static final int RANK_BLOCK_LONGS = 512/Long.SIZE;
  public static final int RANK_BLOCK_BITS = 9;
  public static final int RANKS_PER_BLOCK = BLOCK/RANK_BLOCK;

  private PackedInts.Reader rank;   // One every 512 docs, sparsely represented as not all blocks are DENSE
  private long[] blockCache = null; // One every 65536 docs, contains index & slice position
  public String creationStats = ""; // TODO: Definitely not the way to keep the stats, but where to send them?
  public String name; // Identifier for debug, log & inspection

  // Flags for not-yet-defined-values used during building
  private static final long BLOCK_EMPTY_INDEX = ~0L << BLOCK_INDEX_SHIFT;
  private static final long BLOCK_EMPTY_LOOKUP = BLOCK_LOOKUP_MASK;
  private static final long BLOCK_EMPTY = BLOCK_EMPTY_INDEX | BLOCK_EMPTY_LOOKUP;

  /**
   * Builds the stated caches for the given Indexed
   *
   * @param in positioned at the start of the logical underlying bitmap.
   */
  IndexedDISICache(IndexInput in, boolean createBlockCache, boolean createRankCache, String name) throws IOException {
    if (createBlockCache) {
      blockCache = new long[16];    // Will be extended when needed
      Arrays.fill(blockCache, BLOCK_EMPTY);
    }
    if (!createBlockCache && !createRankCache) {
      return; // Nothing to do
    }
    this.name = name;
    updateCaches(in, createBlockCache, createRankCache);
  }

  private IndexedDISICache() {
    this.blockCache = null;
    this.rank = null;
    this.name = "";
  }
  // TODO: EMPTY works poorly with name, but creating multiple EMPTYs with different names seems wasteful
  public static final IndexedDISICache EMPTY = new IndexedDISICache();

  /**
   * If available, returns a position within the underlying {@link IndexInput} for the start of the block
   * containing the wanted bit (the target) or the next non-EMPTY block, if the block representing the bit is empty.
   * @param targetBlock the index for the block to resolve (docID / 65536).
   * @return the offset for the block for target or -1 if it cannot be resolved.
   */
  public long getFilePointerForBlock(int targetBlock) {
    long offset = !IndexedDISICacheFactory.BLOCK_CACHING_ENABLED ||
        blockCache == null || blockCache.length <= targetBlock ?
        -1 : blockCache[targetBlock] & BLOCK_LOOKUP_MASK;
    return offset == BLOCK_EMPTY_LOOKUP ? -1 : offset;
  }

  /**
   * If available, returns the index; number of set bits before the wanted block.
   * @param targetBlock the block to resolve (docID / 65536).
   * @return the index for the block or -1 if it cannot be resolved.
   */
  public int getIndexForBlock(int targetBlock) {
    if (!IndexedDISICacheFactory.BLOCK_CACHING_ENABLED || blockCache == null || blockCache.length <= targetBlock) {
      return -1;
    }
    return (blockCache[targetBlock] & BLOCK_INDEX_MASK) == BLOCK_EMPTY_INDEX ?
        -1 : (int)(blockCache[targetBlock] >>> BLOCK_INDEX_SHIFT);
  }

  /**
   * Given a target (docID), this method returns the docID
   * @param target the docID for which an index is wanted.
   * @return the docID where the rank is known. This will be lte target.
   */
  // TODO: This method requires way too much knowledge of the intrinsics of the cache. Usage should be simplified
  public int denseRankPosition(int target) {
       return target >> RANK_BLOCK_BITS << RANK_BLOCK_BITS;
  }

  public boolean hasOffsets() {
    return blockCache != null;
  }

  public boolean hasRank() {
    return rank != null;
  }
  
  /**
   * Get the rank (index) for all set bits up to just before the given rankPosition in the block.
   * The caller is responsible for deriving the count of bits up to the docID target from the rankPosition.
   * The caller is also responsible for keeping track of set bits up to the current block.
   * Important: This only accepts rankPositions that aligns to {@link #RANK_BLOCK} boundaries.
   * Note 1: Use {@link #denseRankPosition(int)} to obtain a calid rankPosition for a wanted docID.
   * Note 2: The caller should seek to the rankPosition in the underlying slice to keep everything in sync.
   * @param rankPosition a docID target that aligns to {@link #RANK_BLOCK}.
   * @return the rank (index / set bits count) up to just before the given rankPosition.
   *         If rank is disabled, -1 is returned.
   */
  // TODO: This method requires way too much knowledge of the intrinsics of the cache. Usage should be simplified
  public int getRankInBlock(int rankPosition) {
    if (!IndexedDISICacheFactory.DENSE_CACHING_ENABLED || rank == null) {
      return -1;
    }
    assert rankPosition == denseRankPosition(rankPosition);
    int rankIndex = rankPosition >> RANK_BLOCK_BITS;
    return rankIndex >= rank.size() ? -1 : (int) rank.get(rankIndex);
  }

  private void updateCaches(IndexInput slice, boolean fillBlockCache, boolean fillRankCache)
      throws IOException {
    final long startOffset = slice.getFilePointer();

    final long startTime = System.nanoTime();
    AtomicInteger statBlockALL = new AtomicInteger(0);
    AtomicInteger statBlockDENSE = new AtomicInteger(0);
    AtomicInteger statBlockSPARSE = new AtomicInteger(0);

    // Fill phase
    int largestBlock;
    try {
      largestBlock = fillCache(slice, fillBlockCache, fillRankCache, statBlockALL, statBlockDENSE, statBlockSPARSE);
    } catch (Exception e) { // TODO (Toke): Development debug only. Remove when stable
      creationStats = "Exception filling cache with slice of length " + slice.getFilePointer();
      System.err.println(creationStats);
      e.printStackTrace();
      blockCache = null;
      rank = null;
      slice.seek(startOffset); // Leave it as we found it
      return;
    }

    freezeCaches(fillBlockCache, fillRankCache, largestBlock);

    slice.seek(startOffset); // Leave it as we found it
    creationStats = String.format(
        "name=%s, blocks=%d (ALL=%d, DENSE=%d, SPARSE=%d, EMPTY=%d), time=%dms, block=%b (%d bytes), rank=%b (%d bytes)",
        name,
        largestBlock+1, statBlockALL.get(), statBlockDENSE.get(), statBlockSPARSE.get(),
        (largestBlock+1-statBlockALL.get()-statBlockDENSE.get()-statBlockSPARSE.get()),
        (System.nanoTime()-startTime)/1000000,
        fillBlockCache, blockCache == null ? 0 : blockCache.length*Long.BYTES,
        fillRankCache, rank == null ? 0 : rank.ramBytesUsed());
  }

  private int fillCache(IndexInput slice, boolean fillBlockCache, boolean fillRankCache,
                        AtomicInteger statBlockALL, AtomicInteger statBlockDENSE, AtomicInteger statBlockSPARSE)
      throws IOException {
    char[] buildRank = new char[256];
    int largestBlock = -1;
    long index = 0;
    int rankIndex = -1;
    int rankCountTemp = 0;
    while (slice.getFilePointer() < slice.length()) {
      final long startFilePointer = slice.getFilePointer();

      final int blockIndex = Short.toUnsignedInt(slice.readShort());
      final int numValues = 1 + Short.toUnsignedInt(slice.readShort());

      assert blockIndex > largestBlock;
      if (blockIndex == DocIdSetIterator.NO_MORE_DOCS >>> 16) { // End reached
        assert Short.toUnsignedInt(slice.readShort()) == (DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        break;
      }
      largestBlock = blockIndex;

      if (fillBlockCache) {
        blockCache = ArrayUtil.grow(blockCache, blockIndex+1); // No-op if large enough
        blockCache[blockIndex] = (index << BLOCK_INDEX_SHIFT) | startFilePointer;
      }
      index += numValues;

      if (numValues <= MAX_ARRAY_LENGTH) { // SPARSE
        statBlockSPARSE.incrementAndGet();
        slice.seek(slice.getFilePointer() + (numValues << 1));
        continue;
      }
      if (numValues == 65536) { // ALL
        statBlockALL.incrementAndGet();
        // Already at next block offset
        continue;
      }

      // The block is DENSE
      statBlockDENSE.incrementAndGet();
      long nextBlockOffset = slice.getFilePointer() + (1 << 13);
      if (fillRankCache) {
        int setBits = 0;
        int rankOrigo = blockIndex << 16 >> 9; // Double shift for clarity: The compiler will simplify it
        for (int rankDelta = 0 ; rankDelta < RANKS_PER_BLOCK ; rankDelta++) { // 128 rank-entries in a block
          rankIndex = rankOrigo + rankDelta;
          buildRank = ArrayUtil.grow(buildRank, rankIndex+1);
          buildRank[rankIndex] = (char)setBits;
          rankCountTemp++;
          for (int i = 0 ; i < 512/64 ; i++) { // 8 longs for each rank-entry
            setBits += Long.bitCount(slice.readLong());
          }
        }
        assert slice.getFilePointer() == nextBlockOffset;
      } else {
        slice.seek(nextBlockOffset);
      }
    }
    // Compress the buildRank as it is potentially very sparse
    if (rankIndex < 0) {
      rank = null;
    } else {
      PackedInts.Mutable ranks = PackedInts.getMutable(rankIndex, 16, PackedInts.DEFAULT); // Char = 16 bit
      for (int i = 0 ; i < rankIndex ; i++) {
        ranks.set(i, buildRank[i]);
      }
      rank = LongCompressor.compress(ranks);
    }

    //maxDocID = ((largestBlock+1) << BLOCK_BITS)-1;
    return largestBlock;
  }

  private void freezeCaches(boolean fillBlockCache, boolean fillRankCache, int largestBlock) {
    if (largestBlock == -1) { // No set bit: Disable the caches
      blockCache = null;
      rank = null;
      return;
    }

    // Reduce size to minimum
    if (fillBlockCache && blockCache.length-1 > largestBlock) {
      long[] newBC = new long[Math.max(largestBlock - 1, 1)];
      System.arraycopy(blockCache, 0, newBC, 0, newBC.length);
      blockCache = newBC;
    }
/*    if (fillRankCache && rank.s > (largestBlock+1)*RANKS_PER_BLOCK) {
      char[] newRank = new char[(largestBlock+1)*RANKS_PER_BLOCK];
      System.arraycopy(rank, 0, newRank, 0, newRank.length);
      rank = newRank;
    }*/

    // Replace non-defined values with usable ones
    if (fillBlockCache) {

      // Set non-defined blockCache entries (caused by blocks with 0 set bits) to the subsequently defined one
      long latest = BLOCK_EMPTY;
      for (int i = blockCache.length-1; i >= 0 ; i--) {
        long current = blockCache[i];
        if (current == BLOCK_EMPTY) {
          blockCache[i] = latest;
        } else {
          latest = current;
        }
      }

/*      // Set non-defined blockCache entries (caused by blocks with 0 set bits) to the next defined offset
      long latestLookup = BLOCK_EMPTY_LOOKUP;
      for (int i = blockCache.length-1; i >= 0 ; i--) {
        long currentLookup = blockCache[i] & BLOCK_LOOKUP_MASK;
        if (currentLookup == BLOCK_EMPTY_LOOKUP) { // If empty, set the pointer to the sub-sequent defined one
          blockCache[i] = (blockCache[i] & BLOCK_INDEX_MASK) | (latestLookup & BLOCK_LOOKUP_MASK);
        } else {
          latestLookup = currentLookup;
        }
      }
  */
/*      // Set non-defined index (caused by blocks with 0 set bits) to the previous origo
      long lastIndex = 0L;
      for (int i = 0 ; i < blockCache.length ; i++) {
        long currentIndex = blockCache[i] & BLOCK_INDEX_MASK;
        if (currentIndex == BLOCK_EMPTY_INDEX) {
          blockCache[i] = lastIndex | (blockCache[i] & BLOCK_LOOKUP_MASK);
        } else {
          lastIndex = currentIndex;
        }
      }*/
    }
  }

  @Override
  public long ramBytesUsed() {
    return (blockCache == null ? 0 : RamUsageEstimator.sizeOf(blockCache)) +
        (rank == null ? 0 : rank.ramBytesUsed()) +
        RamUsageEstimator.NUM_BYTES_OBJECT_REF*3 +
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;
  }
}
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
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

import static org.apache.lucene.codecs.lucene70.IndexedDISI.MAX_ARRAY_LENGTH;

/**
 * Caching of IndexedDISI with two strategies:
 *
 * A lookup table for block offsets and a rank structure for DENSE block lookups.
 *
 * The lookup table is an array of {@code long}s with an entry for each block (65536 bits).
 * Each long contains an offset in the underlying IndexSlice, usable for direct jump to
 * the needed block.
 * The alternative to using the lookup table is iteration of all blocks up to the wanted one.
 * The cache overhead is numDocs/1024 bytes.
 * Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits).
 * Non-existing blocks are marked with offset -1.
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
 * ALL or SPARSE. // TODO: Support sparse rank structures
 */
public class IndexedDISICache {
  private static final int RANKS_PER_BLOCK = 65536/512;

  private long[] blockOffsets = null; // One every 65536 docs, contains slice position
  private char[] rank;        // One every 512 docs

  /**
   * Builds the stated caches for the given Indexed
   *
   * @param in positioned at the start of the logical underlying bitmap.
   */
  IndexedDISICache(IndexInput in, int numDocs, boolean createBlockCache, boolean createRankCache) throws IOException {
    if (createBlockCache) {
      blockOffsets = new long[16]; // Will be extended when needed
      Arrays.fill(blockOffsets, -1L); // -1 signals empty (the default)
    }
    rank = createRankCache ? new char[256] : null; // Will be extended when needed
    if (!createBlockCache && !createRankCache) {
      return; // Nothing to do
    }

    fillCaches(in, createBlockCache, createRankCache);
  }

  /**
   * If available, returns a position within the underlying {@link IndexInput} for the start of the block
   * containing the wanted bit (the target) or the next non-EMPTY block, if the block representing the bit is empty.
   * @param target the docID that shall be resolved.
   * @return the offset for the block for target or -1 if it cannot be resolved.
   */
  public long getFilePointer(int target) {
    final int blockTarget = target >> 16;
    return blockOffsets == null || blockOffsets.length <= blockTarget ? -1 : blockOffsets[blockTarget];
  }

  // TODO: Add a method that updates the slice-position and returns doc & index (maybe as a long?

  private void fillCaches(IndexInput slice, boolean fillBlockCache, boolean fillRankCache)
      throws IOException {

    // Fill phase
    int largestBlock = -1;
    while (slice.getFilePointer() < slice.length()) {
      final long startFilePointer = slice.getFilePointer();

      final int blockIndex = Short.toUnsignedInt(slice.readShort());
      final int numValues = 1 + Short.toUnsignedInt(slice.readShort());

      assert blockIndex > largestBlock;
      largestBlock = blockIndex;

      if (fillBlockCache) {
        blockOffsets = ArrayUtil.grow(blockOffsets, blockIndex); // No-op if large enough
        blockOffsets[blockIndex] = startFilePointer;
      }

      if (numValues <= MAX_ARRAY_LENGTH) { // SPARSE
        slice.seek(slice.getFilePointer() + (numValues << 1));
        continue;
      }
      if (numValues == 65536) { // ALL
        // Already at next block offset
        continue;
      }

      // The block is DENSE
      long nextBlockOffset = slice.getFilePointer() + (1 << 13);
      if (fillRankCache) {
        int setBits = 0;
        int rankOrigo = blockIndex << 16 >> 9; // Double shift for clarity: The compiler will simplify it
        for (int rankDelta = 0 ; rankDelta < RANKS_PER_BLOCK ; rankDelta++) { // 128 rank-entries in a block
          final int rankIndex = rankOrigo + rankDelta;
          rank = ArrayUtil.grow(rank, rankIndex);
          rank[rankIndex] = (char)setBits;
          for (int i = 0 ; i < 512/64 ; i++) { // 8 longs for each rank-entry
            setBits += Long.bitCount(slice.readLong());
          }
        }
      }
      assert slice.getFilePointer() == nextBlockOffset;
    }

    // Reduce & polish phase
    if (largestBlock == -1) { // No set bit: Disable the caches
      // TODO: Maybe signal a warning somehow?
      blockOffsets = null;
      rank = null;
      return;
    }

    if (fillBlockCache && blockOffsets.length-1 > largestBlock) {
      long[] newBlockOffsets = new long[largestBlock - 1];
      System.arraycopy(blockOffsets, 0, newBlockOffsets, 0, newBlockOffsets.length);
      blockOffsets = newBlockOffsets;
    }

    if (fillBlockCache) {
      long latestValid = -1;
      for (int i = blockOffsets.length-1 ; i >= 0 ; i--) {
        if (blockOffsets[i] == -1) { // -1 means EMPTY block, so skip to the next valid one
          blockOffsets[i] = latestValid;
        } else {
          latestValid = blockOffsets[i];
        }
      }
    }

    if (fillRankCache && rank.length/RANKS_PER_BLOCK > largestBlock) {
      char[] newRank = new char[largestBlock/RANKS_PER_BLOCK];
      System.arraycopy(rank, 0, newRank, 0, newRank.length);
      rank = newRank;
    }
  }

}
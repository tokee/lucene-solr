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
 * The rank structure for DENSE blocks is an array of {@link long}s with an entry for each
 * sub-block of 2048 bits out of the 65536 bits.
 * Each long is logically divided:
 * The first 32 bits contains the total number of set bits up until before the start of the
 * sub-block in the full IndexedDISI bitmap. Masking to the lower 16 bits gives the total
 * number of set bits up up until before the start of the sub-block within the 65536 bit block.
 * Following this are 3*10 bits, stating the number of set bits within the first three 512 bit
 * sub-sub-blocks in the 2048 bit sub-block.
 * Getting the number of set bits for address 65535 (worst-case) for a DENSE sub-block:
 * 1) Retrieve the {@code long} rankEntry at {@code rank[31]}
 * 2) Set {@code bitCount = rankEntry >>> 32}
 * 3) Set {@code bitCount += rankEntry >>> 22 & 1023 + rankEntry >>> 12 & 1023 + rankEntry >>> 2 & 1023}
 * 4) Set {@code bitCount += Long.bitCount(block[index])} with index 1016...1023
 * See https://en.wikipedia.org/wiki/Succinct_data_structure for details on rank structures.
 * The alternative to using the rank structure is iteration and summing of set bits for all
 * entries in the DENSE sub-block up until the wanted bit, with a worst-case of 1024 entries.
 * The rank cache overhead for a single DENSE block is 32 longs or 1/32th of the sub-block size.
 *
 * The total overhead for the rank cache is currently also numDocs/32 bits or numDocs/8 bytes
 * as the rank-representation is not sparse itself, using empty entries for sub-blocks of type
 * ALL or SPARSE. // TODO: Supporte sparse rank structures
 */
public class IndexedDISICache {
  private final long[] blockOffsets;
  private final long[] rank;

  /**
   * Builds the stated caches for the given Indexed
   *
   * @param in positioned at the start of the logical underlying bitmap.
   */
  IndexedDISICache(IndexInput in, int numDocs, boolean createBlockCache, boolean createRankCache) throws IOException {
    blockOffsets = createBlockCache ? new long[numDocs >> 16] : null; // One every 65536 docs
    rank = createRankCache ? new long[numDocs >> 11] : null;           // One every 2048 docs
    if (!createBlockCache && !createRankCache) {
      return; // Nothing to do
    }

    //fillCaches(in, numDocs, createBlockCache, createRankCache);
  }

  enum IndexedMethod {ALL, DENSE, SPARSE }
  // Extracted and reduced from IndexedDISI
  private class disiIterator {
    private final IndexInput slice;

    public disiIterator(IndexInput slice) {
      this.slice = slice;
    }

    private int block = -1;
    private long blockEnd;
    private int nextBlockIndex = -1;
    IndexedMethod method;

    private int doc = -1;
    private int index = -1;

    // DENSE variables
    private long word;
    private int wordIndex = -1;
    // number of one bits encountered so far, including those of `word`
    private int numberOfOnes;

    // ALL variables
    private int gap;

    public int docID() {
      return doc;
    }

    private void advanceBlock(int targetBlock) throws IOException {
      do {
        slice.seek(blockEnd);
        readBlockHeader();
      } while (block < targetBlock);
    }

    private void readBlockHeader() throws IOException {
      block = Short.toUnsignedInt(slice.readShort()) << 16;
      assert block >= 0;
      final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
      index = nextBlockIndex;
      nextBlockIndex = index + numValues;
      if (numValues <= MAX_ARRAY_LENGTH) {
        method = IndexedMethod.SPARSE;
        blockEnd = slice.getFilePointer() + (numValues << 1);
      } else if (numValues == 65536) {
        method = IndexedMethod.ALL;
        blockEnd = slice.getFilePointer();
        gap = block - index - 1;
      } else {
        method = IndexedMethod.DENSE;
        blockEnd = slice.getFilePointer() + (1 << 13);
        wordIndex = -1;
        numberOfOnes = index + 1;
      }
    }

    public int index() {
      return index;
    }


    boolean denseAdvanceWithinBlock(int target) throws IOException {
      final int targetInBlock = target & 0xFFFF;
      final int targetWordIndex = targetInBlock >>> 6;
      for (int i = wordIndex + 1; i <= targetWordIndex; ++i) {
        word = slice.readLong();
        numberOfOnes += Long.bitCount(word);
      }
      wordIndex = targetWordIndex;

      long leftBits = word >>> target;
      if (leftBits != 0L) {
        doc = target + Long.numberOfTrailingZeros(leftBits);
        index = numberOfOnes - Long.bitCount(leftBits);
        return true;
      }

      while (++wordIndex < 1024) {
        word = slice.readLong();
        if (word != 0) {
          index = numberOfOnes;
          numberOfOnes += Long.bitCount(word);
          doc = block | (wordIndex << 6) | Long.numberOfTrailingZeros(word);
          return true;
        }
      }
      return false;
    }

    boolean advanceExactWithinBlock(int target) throws IOException {
      final int targetInBlock = target & 0xFFFF;
      final int targetWordIndex = targetInBlock >>> 6;
      for (int i = wordIndex + 1; i <= targetWordIndex; ++i) {
        word = slice.readLong();
        numberOfOnes += Long.bitCount(word);
      }
      wordIndex = targetWordIndex;

      long leftBits = word >>> target;
      index = numberOfOnes - Long.bitCount(leftBits);
      return (leftBits & 1L) != 0;
    }
  }
    /*
  private void fillCaches(IndexInput slice, int numDocs, boolean fillBlockCache, boolean createRankCache)
      throws IOException {
    int index = 0;
    int setBits = 0;
    long nextBlockOffset = 0;
    long blockOffset = 0;

    if (fillBlockCache) {
      Arrays.fill(blockOffsets, -1L); // Default is no set bits
    }

    while (index < numDocs) {
      assert index >> 16 << 16 == 0; // Blocks are 2^16 bits
      if (fillBlockCache) {
        blockOffsets[index >> 16] = blockOffset;
      }

      // Move to next block
      slice.seek(nextBlockOffset);
      blockOffset = nextBlockOffset;

// Get basic data
      final int blockIndex = Short.toUnsignedInt(slice.readShort()) << 16;
      assert blockIndex == index >> 16;
      final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
      IndexedMethod method;
      if (numValues <= MAX_ARRAY_LENGTH) {
        method = IndexedMethod.SPARSE;
        nextBlockOffset = slice.getFilePointer() + (numValues << 1);
      } else if (numValues == 65536) {
        method = IndexedMethod.ALL;
        nextBlockOffset = slice.getFilePointer();
      } else {
        method = IndexedMethod.DENSE;
        nextBlockOffset = slice.getFilePointer() + (1 << 13);
      }

      if (method != IndexedMethod.DENSE) { // Only DENSE is ranked
        index += 1 << 16;
        continue;
      }

      // The block is DENSE, so rank it
    }

  }
  */
}
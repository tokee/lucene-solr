package org.apache.lucene.util;

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


/**
 * Wrapper for OpenBitSet which creates and exposes a rank cache.
 * </p><p>
 * A rank-cache allows for fast counting of the number of set bits up to, but not including,
 * a the bit at a given position.
 * </p><p>
 * The rankCache uses a long for every 2048 bits in the underlying bitset and thus has an overhead of 3.17%.
 * Performance is O(1):
 * 1 lookup in the {@code long[]} that holds the rank-data,
 * a maximum of 3 sums,
 * a maximum of 8 Long.bitCounts.
 * </p>
 * The design is based heavily on the article
 * Space-Efficient, High-Performance Rank & Select Structures on Uncompressed Bit Sequences
 * by Dong Zhou, David G. Andersen, Michael Kaminsky, Carnegie Mellon University, Intel Labs
 * http://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf
 */
public class RankBitSet extends OpenBitSet {
  public static final int  LOWER_BITS = 32; // Must be capable of addressing full Java array
  public static final long LOWER_MASK = ~(~1L << (LOWER_BITS-1));
  public static final int LOWER_OVER_BITS = 11;
  public static final long LOWER_OVER_MASK = ~(~1L << (LOWER_OVER_BITS-1));
  public static final int LOWER_OVER_SIZE = 2048; // Overflow bits handled by a lower block

  public static final int BASIC_BITS = 10; // Needs to hold counts from 0-512 (513-1023 are never used)
  public static final long BASIC_MASK = ~(~1L << (BASIC_BITS-1));
  public static final int BASIC_OVER_BITS = 9;
  public static final long BASIC_OVER_MASK = ~(~1L << (BASIC_OVER_BITS-1));
  public static final int BASIC_OVER_SIZE = 512; // Overflow bits handled by a basic block
  public static final int BASIC_WORDS = BASIC_OVER_SIZE /Long.SIZE; // word == long
  /**
   * Each entry is made up of 1 long<br/>
   * Bits 63-32: 32 bit first-level absolute index.<br/>
   * Bits 29-0: 3 * 10 bit (0-1023) second-level relative index. Only numbers 0-512 are used.
   */
  private long[] rankCache = null;

  public RankBitSet(long numBits) {
    super(numBits);
  }

  public RankBitSet() { }

  public RankBitSet(long[] bits, int numWords) {
    super(bits, numWords);
  }

  /**
   * Must be called after bits has changed and before {@link #rank} is called.
   */
  public void buildRankCache() {
    rankCache = new long[(int) (size() >>> LOWER_OVER_BITS)+1];
    long total = 0;
    int lower = 0 ;
    while (lower * LOWER_OVER_SIZE < size()) { // Full lower block processing
      final int origoWordIndex = (lower * LOWER_OVER_SIZE) >>> 6;
    // TODO: Some conditionals could be spared by checking once if all basic blocks are within size
      final long basic1 = origoWordIndex + BASIC_WORDS <= wlen ?
          BitUtil.pop_array(getBits(), origoWordIndex, BASIC_WORDS) : 0;
      final long basic2 =  origoWordIndex + BASIC_WORDS*2 <= wlen ?
          BitUtil.pop_array(getBits(), origoWordIndex + BASIC_WORDS, BASIC_WORDS) : 0;
      final long basic3 =  origoWordIndex + BASIC_WORDS*3 <= wlen ?
          BitUtil.pop_array(getBits(), origoWordIndex + BASIC_WORDS *2, BASIC_WORDS) : 0;
      final long basic4 =  origoWordIndex + BASIC_WORDS*4 <= wlen ?
          BitUtil.pop_array(getBits(), origoWordIndex + BASIC_WORDS *3, BASIC_WORDS) : 0;
      rankCache[lower] = total << (Long.SIZE-LOWER_BITS) |
           basic1 << (BASIC_BITS *2) |
           basic2 << BASIC_BITS |
           basic3;
      total += basic1 + basic2 + basic3 + basic4;
      lower++;
    }
  }

  public int rank(long index) {
    final long cache = rankCache[((int) (index >>> LOWER_OVER_BITS))];
    // lower cache (absolute)
    long rank = cache >>> (Long.SIZE-LOWER_BITS);
    int startBitIndex = (int) (index & ~LOWER_OVER_MASK);
    // basic blocks (relative)
    if (startBitIndex < index-BASIC_OVER_SIZE) {
      rank += (cache >>> (BASIC_BITS*2)) & BASIC_MASK;
      startBitIndex += BASIC_OVER_SIZE;
      if (startBitIndex < index-BASIC_OVER_SIZE) {
        rank += (cache >>> BASIC_BITS) & BASIC_MASK;
        startBitIndex += BASIC_OVER_SIZE;
        if (startBitIndex < index-BASIC_OVER_SIZE) {
          rank += cache & BASIC_MASK;
          startBitIndex += BASIC_OVER_SIZE;
        }
      }
    }
    // long.bitcount (relative)
    while(startBitIndex < index-Long.SIZE) {
      rank += Long.bitCount(getBits()[startBitIndex >>> 6]);
      startBitIndex += Long.SIZE;
    }
    // Single bits (relative)
    if (startBitIndex < index) {
/*      System.out.println(String.format(Locale.ENGLISH,
          "startBitIndex=%d, index=%d, getBits()[startBitIndex>>>6=%d]=%s, index-startBitIndex=%d, mask=%s",
          startBitIndex, index, startBitIndex>>>6, Long.toBinaryString(getBits()[startBitIndex>>>6]),
          index-startBitIndex, Long.toBinaryString(~(~1L << (index-startBitIndex-1)))));*/
      rank += Long.bitCount(getBits()[startBitIndex >>> 6] & ~(~1L << (index-startBitIndex-1)));
    }
//    for (int i = startBitIndex ; i < index ; i++) {
//      rank += fastGet(i) ? 1 : 0;
//    }
    return (int) rank;
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF*2 +
        RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_LONG) +
        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_LONG*bits.length +
        (rankCache == null ? 0 :
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_LONG*rankCache.length);
  }
}

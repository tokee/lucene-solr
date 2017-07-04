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

package org.apache.solr.search.sparse.counters.plane;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Rank cache tightly coupled to {@link FixedBitSet} and {@link LongBitSet}.
 *
 * A rank-cache allows for fast counting of the number of set bits up to, but not including,
 * a given position in the bit set. Building the rank-cache takes time and must be done
 * anew if the underlying bit set has changed.
 *
 * The RankCache has a long for every 2048 bits and thus has an overhead of 3.17%.
 * Performance is O(1):
 * 1 long[index]-lookup in the rank cache,
 * a maximum of 3 sums,
 * a maximum of 8 Long.bitCounts, each requiring a long[index]-lookup in the bit set data.
 * As the lookups in the bit set data are always sequential, there are at most 3 memory-cache misses
 * (1 for the rank cache, 1-2 for the bit set data). {@link Long#bitCount(long)} a single CPU
 * instruction {code popcnt} with Java 1.8 and newer CPUs (Intel & AMD 2008+).
 *
 * The design is based heavily on the article
 * Space-Efficient, High-Performance Rank & Select Structures on Uncompressed Bit Sequences
 * by Dong Zhou, David G. Andersen, Michael Kaminsky, Carnegie Mellon University, Intel Labs
 * http://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf
 */
public class RankCache implements Accountable {
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
  private final long[] rankCache;

  // We need access to the underlying long[], so we cannot have {@link BitSet} as inner bitSet.
  // {@link LongBitSet} also exposes its long[], so RankCache could be extended to support both.
  // Unfortunately the {@link LongBitSet} is not a {@code BitSet}, so it is hard to add support without
  // sacrificing performance.
  //
  private final int numWords;
  private final long[] bits;
  private final long numBits;

  /**
   * Create a rank cache backed by a FixedBitSet, ready for use.
   * If the fixedBitSet is modified, {@link #buildRankCache()} must be called before calling {@link #rank(long)}.
   * @param fixedBitSet the underlying bit set for the rank cache.
   */
  public RankCache(FixedBitSet fixedBitSet) {
    this(fixedBitSet, true);
  }
  /**
   * Create a rank cache backed by a FixedBitSet, optionally building the internal rank structures.
   * {@link #buildRankCache()} must be called before calling {@link #rank(long)}.
   * @param fixedBitSet the underlying bit set for the rank cache.
   * @param buildRankCache if true, the internal rank structures will be build.
   *                       If false, the caller is responsible for activating cache build before use.
   */
  public RankCache(FixedBitSet fixedBitSet, boolean buildRankCache) {
    rankCache = new long[(fixedBitSet.length() >>> LOWER_OVER_BITS)+1];
    numWords = FixedBitSet.bits2words(fixedBitSet.length());
    bits = fixedBitSet.getBits();
    numBits = fixedBitSet.length();
    if (buildRankCache) {
      buildRankCache();
    }
  }

  /**
   * Create a rank cache backed by a LongBitSet, ready for use.
   * If the fixedBitSet is modified, {@link #buildRankCache()} must be called before calling {@link #rank(long)}.
   * @param longBitSet the underlying bit set for the rank cache.
   */
  public RankCache(LongBitSet longBitSet) {
    this(longBitSet, true);
  }
  /**
   * Create a rank cache backed by a LongBitSet, optionally building the internal rank structures.
   * {@link #buildRankCache()} must be called before calling {@link #rank(long)}.
   * @param longBitSet the underlying bit set for the rank cache.
   * @param buildRankCache if true, the internal rank structures will be build.
   *                       If false, the caller is responsible for activating cache build before use.
   */
  public RankCache(LongBitSet longBitSet, boolean buildRankCache) {
    rankCache = new long[(int) ((longBitSet.length() >>> LOWER_OVER_BITS)+1)];
    numWords = LongBitSet.bits2words(longBitSet.length());
    bits = longBitSet.getBits();
    numBits = longBitSet.length();
    if (buildRankCache) {
      buildRankCache();
    }
  }

  /**
   * Must be called after bits has changed and before {@link #rank} is called.
   * This is called automatically by the RankCache constructors.
   */
  public void buildRankCache() {
    long total = 0;
    int lower = 0 ;
    while (lower * LOWER_OVER_SIZE < numBits) { // Full lower block processing
      final int origoWordIndex = (lower * LOWER_OVER_SIZE) >>> 6;
    // TODO: Some conditionals could be spared by checking once if all basic blocks are within size
      final long basic1 = origoWordIndex + BASIC_WORDS <= numWords ?
          BitUtil.pop_array(bits, origoWordIndex, BASIC_WORDS) : 0;
      final long basic2 =  origoWordIndex + BASIC_WORDS*2 <= numWords ?
          BitUtil.pop_array(bits, origoWordIndex + BASIC_WORDS, BASIC_WORDS) : 0;
      final long basic3 =  origoWordIndex + BASIC_WORDS*3 <= numWords ?
          BitUtil.pop_array(bits, origoWordIndex + BASIC_WORDS *2, BASIC_WORDS) : 0;
      final long basic4 =  origoWordIndex + BASIC_WORDS*4 <= numWords ?
          BitUtil.pop_array(bits, origoWordIndex + BASIC_WORDS *3, BASIC_WORDS) : 0;
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
      rank += Long.bitCount(bits[startBitIndex >>> 6]);
      startBitIndex += Long.SIZE;
    }
    // Single bits (relative)
    if (startBitIndex < index) {
/*      System.out.println(String.format(Locale.ENGLISH,
          "startBitIndex=%d, index=%d, getBits()[startBitIndex>>>6=%d]=%s, index-startBitIndex=%d, mask=%s",
          startBitIndex, index, startBitIndex>>>6, Long.toBinaryString(getBits()[startBitIndex>>>6]),
          index-startBitIndex, Long.toBinaryString(~(~1L << (index-startBitIndex-1)))));*/
      rank += Long.bitCount(bits[startBitIndex >>> 6] & ~(~1L << (index-startBitIndex-1)));
    }
//    for (int i = startBitIndex ; i < index ; i++) {
//      rank += fastGet(i) ? 1 : 0;
//    }
    return (int) rank;
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF*2 +
        Integer.BYTES + Long.BYTES) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + Long.BYTES*numWords +
        (rankCache == null ? 0 : RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + Long.BYTES*rankCache.length);
  }

}

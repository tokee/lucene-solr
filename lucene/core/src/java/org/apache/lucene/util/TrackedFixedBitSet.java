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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Low-overhead tracking of set bits, allowing for fast iteration and join-operations between two TrackedFixedBitSets.
 * Adapted from {@link FixedBitSet}.
 * </p><p>
 * TrackedFixedBitSet uses 1 tracking bit for each underlying long (64 bits) and 1 tracking-tracking bit for each 64
 * tracking bits, which translates to a memory overhead of #bits/64/8 + #bits/64/64/8 bytes. The dual-layer tracking
 * ensures that worst-case iteration (1 single set bit at the last position in the bitmap) requires bits/64/64/64
 * lookups. For 100M bits that is 381 lookups.
 * </p><p>
 * TODO: Handle non-sparse better.
 * For dense bitsets, the overhead of iterating the trackers could be removed by iterating all words directly, as
 * {@link org.apache.lucene.util.FixedBitSet} does. Heuristic detection of dense bits could be done fairly fast by
 * counting bits in the top-level tracker {@link #tracker2} as there are only #bits/64/64/64 of those. If the
 * amount of set bits at the top level is above a given yet-to-be-determined level, the bitset is probably dense.
 */
public final class TrackedFixedBitSet extends DocIdSet implements Bits {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TrackedFixedBitSet.class);

  /**
   * A {@link org.apache.lucene.search.DocIdSetIterator} which iterates over set bits in a
   * {@link org.apache.lucene.util.TrackedFixedBitSet}.
   */
  public static final class TrackedFixedBitSetIterator extends DocIdSetIterator {
    final TrackedFixedBitSet source;
    final int numBits, numWords;
    final long[] bits;
    final long[] t1;
    final long[] t2;
    int doc = -1;

    /** Creates an iterator over the given {@link org.apache.lucene.util.TrackedFixedBitSet}. */
    public TrackedFixedBitSetIterator(TrackedFixedBitSet bitset) {
      this.bits = bitset.bits;
      this.numBits = bitset.numBits;
      this.t1 = bitset.tracker1;
      this.t2 = bitset.tracker2;
      this.numWords = bitset.numWords;
      this.source = bitset;
    }

    @Override
    public int nextDoc() {
      if (doc == NO_MORE_DOCS || ++doc >= numBits) {
        return doc = NO_MORE_DOCS;
      }
      int i = doc >> 6;
      long word = bits[i] >> doc;  // skip all the bits to the right of index

      if (word != 0) {
        return doc = doc + Long.numberOfTrailingZeros(word);
      }

    // TODO: Use trackers
      while (++i < numWords) {
        word = bits[i];
        if (word != 0) {
          return doc = (i << 6) + Long.numberOfTrailingZeros(word);
        }
      }

      return doc = NO_MORE_DOCS;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public long cost() {
      return numBits;
    }

    @Override
    public int advance(int target) {
      if (doc == NO_MORE_DOCS || target >= numBits) {
        return doc = NO_MORE_DOCS;
      }
      int i = target >> 6;
      long word = bits[i] >> target; // skip all the bits to the right of index

      if (word != 0) {
        return doc = target + Long.numberOfTrailingZeros(word);
      }

    // TODO: Use trackers
      while (++i < numWords) {
        word = bits[i];
        if (word != 0) {
          return doc = (i << 6) + Long.numberOfTrailingZeros(word);
        }
      }

      return doc = NO_MORE_DOCS;
    }
  }

  public WordIterator wordIterator() {
    return new WordIterator(this);
  }
  public static final class WordIterator {
    public static final int NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS;

    final int numBits, numWords;
    final long[] bits;
    final long[] tracker1;
    final long[] tracker2;

    int t1Num = -1;
    int t2Num = -1;
    int wordNum = -1;

    long t1Bitset, t2Bitset;

    public WordIterator(TrackedFixedBitSet bits) {
      this(bits.bits, bits.numBits, bits.tracker1, bits.tracker2, bits.numWords);
    }

    /** Creates an iterator over the given array of bits. */
    public WordIterator(long[] bits, int numBits, long[] tracker1, long[] tracker2, int wordLength) {
      this.bits = bits;
      this.numBits = numBits;
      this.numWords = wordLength;
      this.tracker1 = tracker1;
      this.tracker2 = tracker2;
    }

    public int nextWordNum() {
      if (wordNum == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }

      while (t1Bitset == 0) {
        while (t2Bitset == 0) {
          if (++t2Num == tracker2.length) {
            return wordNum = NO_MORE_DOCS;
          }
          t2Bitset = tracker2[t2Num];
        }
        final long t2Magic = t2Bitset & -t2Bitset;
        t1Num = Long.bitCount(t2Magic - 1);
        t2Bitset ^= t2Magic;

        t1Bitset = tracker1[t2Num * 64 + t1Num];
      }
      final long t1Magic = t1Bitset & -t1Bitset;
      wordNum = t2Num*64*64 + t1Num*64 + Long.bitCount(t1Magic - 1);
      t1Bitset ^= t1Magic;
      return wordNum;
    }

    /**
     * Skips the cursor to the next not-0 word at or after the given target. This method utilizes trackers.
     * @param target the first word to iterate from.
     * @return the wordNum for the first non-0 word >= target or {@link #NO_MORE_DOCS}.
     */
    public int advance(final int target) {
      if (target >= numBits) {
        return wordNum = NO_MORE_DOCS;
      }
      // Reset
      t1Bitset = 0;
      t2Bitset = 0;
      wordNum = -1;
      t2Num = target/64/64-1;
      t1Num = -1;

      // Advance the trackers
      while (t2Num == target/64/64-1 && t1Num < target%(64/64)/64) { // Positioned before target
        while (t1Bitset == 0) {
          while (t2Bitset == 0) {
            if (++t2Num == tracker2.length) {
              return wordNum = NO_MORE_DOCS;
            }
            t2Bitset = tracker2[t2Num];
          }
          // How do we fast-forward to target%(64/64)/64?
          final long t2Magic = t2Bitset & -t2Bitset;
          t1Num = Long.bitCount(t2Magic - 1);
          t2Bitset ^= t2Magic;

          t1Bitset = tracker1[t2Num * 64 + t1Num];
        }
      }
      final long t1Magic = t1Bitset & -t1Bitset;
      wordNum = t2Num*64*64 + t1Num*64 + Long.bitCount(t1Magic - 1);
      t1Bitset ^= t1Magic;

      while (wordNum < target) { // Advance the single bits
        if (nextWordNum() == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
      }
      return wordNum;
    }

    public long word() {
      return bits[wordNum];
    }
    public int wordNum() {
      return wordNum;
    }
  }

  @FunctionalInterface
  interface MergeCallback {
    long merge(int wordNum, long word1, long word2);
  }

  /**
   * Iterates through the two given bitsets in parallel, activating callback whenever one of the words are != 0.
   * The iteration is performed with the {@link WordIterator} and as a consequence takes advantage of the tracked
   * nature of TrackedFixedBitSet: Merging of sparse bitsets will be fast.
   * @param tracked1 first bitset, produces word1 in the callback.
   * @param tracked2 second bitset, produces word2 in the callback.
   * @param stopAtFirstDepletion if true, iteration is stopped as soon as the smallest bitset has been depleted.
   * @param callback called for each non-0 word in either of the bitsets.
   * @return sum of the longs produced by callback.
   */
  static long merge(TrackedFixedBitSet tracked1, TrackedFixedBitSet tracked2,
                     boolean stopAtFirstDepletion, MergeCallback callback) {
    long total = 0;
    WordIterator words1 = tracked1.wordIterator();
    WordIterator words2 = tracked2.wordIterator();
    int wordNum1 = words1.nextWordNum();
    int wordNum2 = words2.nextWordNum();
    while (wordNum1 != WordIterator.NO_MORE_DOCS && wordNum2 != WordIterator.NO_MORE_DOCS) {
      if (wordNum1 == wordNum2) {
        total += callback.merge(wordNum1, words1.word(), words2.word());
        wordNum1 = words1.nextWordNum();
        wordNum2 = words2.nextWordNum();
      } else if (wordNum1 < wordNum2) {
        total += callback.merge(wordNum1, words1.word(), 0);
        wordNum1 = words1.nextWordNum();
      } else {
        total += callback.merge(wordNum2, 0, words2.word());
        wordNum2 = words2.nextWordNum();
      }
    }
    if (stopAtFirstDepletion) {
      return total;
    }

    // Empty the last one
    while (wordNum1 != WordIterator.NO_MORE_DOCS) {
      total += callback.merge(wordNum1, words1.word(), 0);
      wordNum1 = words1.nextWordNum();
    }
    while (wordNum2 != WordIterator.NO_MORE_DOCS) {
      total += callback.merge(wordNum2, 0, words2.word());
      wordNum2 = words2.nextWordNum();
    }
    return total;
  }

  /**
   * If the given {@link org.apache.lucene.util.TrackedFixedBitSet} is large enough to hold {@code numBits},
   * returns the given bits, otherwise returns a new {@link org.apache.lucene.util.TrackedFixedBitSet} which
   * can hold the requested number of bits.
   *
   * <p>
   * <b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of
   * the given {@code bits} if possible. Also, calling {@link #length()} on the
   * returned bits may return a value greater than {@code numBits}.
   */
  public static TrackedFixedBitSet ensureCapacity(TrackedFixedBitSet bits, int numBits) {
    if (numBits < bits.length()) {
      return bits;
    } else {
      int numWords = bits2words(numBits);
      long[] arr = bits.getBits();
      if (numWords >= arr.length) {
        arr = ArrayUtil.grow(arr, numWords + 1);
      }
      // TODO: Copy existing tracking bits instead of recreating them
      return new TrackedFixedBitSet(arr, arr.length << 6);
    }
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    int numLong = numBits >>> 6;
    if ((numBits & 63) != 0) {
      numLong++;
    }
    return numLong;
  }

  /**
   * Returns the popcount or cardinality of the intersection of the two sets.
   * Neither set is modified.
   */
  public static long intersectionCount(TrackedFixedBitSet a, TrackedFixedBitSet b) {
    return merge(a, b, true, (wordNum, word1, word2) -> Long.bitCount(word1 & word2));
//    return BitUtil.pop_intersect(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
  }

  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither
   * set is modified.
   */
  public static long unionCount(TrackedFixedBitSet a, TrackedFixedBitSet b) {
    return merge(a, b, false, (wordNum, word1, word2) -> Long.bitCount(word1 | word2));
/*
    long tot = BitUtil.pop_union(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    if (a.numWords < b.numWords) {
      tot += BitUtil.pop_array(b.bits, a.numWords, b.numWords - a.numWords);
    } else if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;*/
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or
   * "intersection(a, not(b))". Neither set is modified.
   */
  public static long andNotCount(TrackedFixedBitSet a, TrackedFixedBitSet b) {
    return merge(a, b, false, (wordNum, word1, word2) -> Long.bitCount(word1 & ~word2));
/*    long tot = BitUtil.pop_andnot(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;*/
  }

  final long[] tracker2;
  final long[] tracker1;
  final long[] bits;
  final int numBits;
  final int numWords;

  public TrackedFixedBitSet(int numBits) {
    this.numBits = numBits;
    bits = new long[bits2words(numBits)];
    tracker1 = new long[bits.length/64+1];
    tracker2 = new long[tracker1.length/64+1];
    numWords = bits.length;
  }

  public TrackedFixedBitSet(long[] storedBits, int numBits) {
    this.numWords = bits2words(numBits);
    if (numWords > storedBits.length) {
      throw new IllegalArgumentException("The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits;
    tracker1 = new long[bits.length/64+1];
    tracker2 = new long[tracker1.length/64+1];
    fillTrackers();
  }

  private TrackedFixedBitSet(long[] storedBits, int numBits, long[] tracker1, long[] tracker2) {
    this.numWords = bits2words(numBits);
    if (numWords > storedBits.length) {
      throw new IllegalArgumentException("The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits;
    this.tracker1 = tracker1;
    this.tracker2 = tracker2;
  }

  /**
   * Assumes trackers are zeroed and fills them from set bits.
   */
  private void fillTrackers() {
    for (int i = 0 ; i < bits.length ; i++) {
      if (bits[i] != 0) {
//        System.out.println("tracker1[" + (i >>> 6) + "](length==" + tracker1.length + ") |= " + (1L << (i & 63)));
        tracker1[i >>> 6] |= 1L << (i & 63);
      }
    }
    // It is faster to do this separately as we then only make 1/64 the number of updates to tracker2
    for (int i = 0 ; i < tracker1.length ; i++) {
      if (tracker1[i] != 0) {
//        System.out.println("tracker2[" + (i >>> 6) + "](length==" + tracker2.length + ") |= " + (1L << (i & 63)));
        tracker2[i >>> 6] |= 1L << (i & 63);
      }
    }
  }

  /**
   * The given word is not-0. Update trackers accordingly.
   * @param wordNum the word in {@link #bits} to update trackers for.
   */
  private void trackWord(int wordNum) {
    final long setBit = 1L << (wordNum & 63);
    if ((tracker1[wordNum >>> 6] |= setBit) == setBit) {
      // Probably the first update in tracker1[wordNum >>> 6]. Trigger level 2 tracking
      tracker2[wordNum >>> 12] |= 1L << ((wordNum >>> 6) & 63);
    }
  }

  private void untrackWord(int wordNum) {
    final long removeBit = 1L << (wordNum & 63);
    if ((tracker1[wordNum >>> 6] &= ~removeBit) == 0) {
      // Probably the first update in tracker1[wordNum >>> 6]. Trigger level 2 tracking
      tracker2[wordNum >>> 12] &= ~(1L << ((wordNum >>> 6) & 63));
    }
  }

  /**
   * Informs the trackers that the word range contains only zeroes.
   * @param startWord inclusive.
   * @param endWord exclusive.
   */
  // TODO: Optimize this method so that Arrays.fill is used for zeroing the trackers
  private void untrackWords(int startWord, int endWord) {
    for (int middleWord = startWord ; middleWord < endWord ; middleWord++) {
      untrackWord(middleWord);
    }
  }
  /**
   * Informs the trackers that the word range contains non-zero values.
   * @param startWord inclusive.
   * @param endWord exclusive.
   */
  // TODO: Optimize this method so that Arrays.fill is used for zeroing the trackers
  private void trackWords(int startWord, int endWord) {
    for (int middleWord = startWord ; middleWord < endWord ; middleWord++) {
      trackWord(middleWord);
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return new TrackedFixedBitSetIterator(this);
  }

  @Override
  public Bits bits() {
    return this;
  }

  @Override
  public int length() {
    return numBits;
  }

  /** This DocIdSet implementation is cacheable. */
  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bits) +
        RamUsageEstimator.sizeOf(tracker1) + RamUsageEstimator.sizeOf(tracker2);
  }

  /** Expert. */
  public long[] getBits() {
    return bits;
  }

  /** Returns number of set bits.  NOTE: this visits every not-0
   *  long in the backing bits array, and the result is not
   *  internally cached! */
  public int cardinality() {
    long cardinality = 0;
    WordIterator words = wordIterator();
    while (words.nextWordNum() != WordIterator.NO_MORE_DOCS) {
      cardinality++;
    }
    return (int) cardinality;
  }

  @Override
  public boolean get(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (bits[i] & bitmask) != 0;
  }

  public void set(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    if ((bits[wordNum] |= bitmask) == bitmask) { // First bit in word (or already set, but that only harms performance)
      trackWord(wordNum);
    }
  }

  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    if ((bits[wordNum] |= bitmask) == bitmask) { // First bit in word (or already set, but that only harms performance)
      trackWord(wordNum);
    }
    return val;
  }

  public void clear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    if ((bits[wordNum] &= ~bitmask) == 0) { // No more bits in word
      untrackWord(wordNum);
    }
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    if ((bits[wordNum] &= ~bitmask) == 0) { // No more bits in word
      untrackWord(wordNum);
    }
    return val;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    // TODO: Use trackers
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;
    long word = bits[i] >> index;  // skip all the bits to the right of index

    if (word!=0) {
      return index + Long.numberOfTrailingZeros(word);
    }

    while(++i < numWords) {
      word = bits[i];
      if (word != 0) {
        return (i<<6) + Long.numberOfTrailingZeros(word);
      }
    }

    return -1;
  }

  /** Returns the index of the last set bit before or on the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int prevSetBit(int index) {
    // TODO: Use trackers
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f;  // index within the word
    long word = (bits[i] << (63-subIndex));  // skip all the bits to the left of index

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = bits[i];
      if (word !=0 ) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  /** Does in-place OR of the bits provided by the
   *  iterator. */
  public void or(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      or(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      or(fbs.source);
//      or(fbs.bits, fbs.numWords);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      fbs.advance(numBits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        set(doc);
      }
    }
  }

  /** this = this OR other */
  public void or(TrackedFixedBitSet other) {
    merge(this, other, false, (wordNum, word1, word2) -> {
      if (word2 != 0) {
        long original = bits[wordNum];
        bits[wordNum] = word1 | word2;
        if (original == 0) {
          trackWord(wordNum);
        }
      }
      return 0;
    });
//    or(other.bits, other.numWords);
  }
  
  private void or(final long[] otherArr, final int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", otherNumWords=" + otherNumWords;
    final long[] thisArr = this.bits;
    int pos = Math.min(numWords, otherNumWords);
    while (--pos >= 0) {
      if ((thisArr[pos] |= otherArr[pos]) != 0) {
        trackWord(pos);
      }
    }
  }

  /** this = this XOR other */
  public void xor(TrackedFixedBitSet other) {
    assert other.numWords <= numWords : "numWords=" + numWords + ", other.numWords=" + other.numWords;
    int limit = Math.min(numWords, other.numWords);
    merge(this, other, false, (wordNum, word1, word2) -> {
      if (wordNum >= limit) {
        return 0; // Some break-out mechanism would be nice
      }
      long original = bits[wordNum];
      bits[wordNum] = word1 ^ word2;
      if (original == 0 && bits[wordNum] != 0) {
        trackWord(wordNum);
      } else if (original != 0 && bits[wordNum] == 0) {
        untrackWord(wordNum);
      }
      return 0; // We only deal in side effects for this
    });

/*
    final long[] thisBits = this.bits;
    final long[] otherBits = other.bits;
    int pos = Math.min(numWords, other.numWords);
    while (--pos >= 0) {
      final long original = thisBits[pos];
      thisBits[pos] ^= otherBits[pos];
      if (original == 0 && thisBits[pos] != 0) {
        trackWord(pos);
      } else if (original != 0 && thisBits[pos] == 0) {
        untrackWord(pos);
      }
    }*/
  }
  
  /** Does in-place XOR of the bits provided by the iterator. */
  public void xor(DocIdSetIterator iter) throws IOException {
    if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      xor(fbs.source);
      fbs.advance(numBits);
      return;
    }

    int doc;
    while ((doc = iter.nextDoc()) < numBits) {
      flip(doc, doc + 1);
    }
  }

  /** Does in-place AND of the bits provided by the
   *  iterator. */
  public void and(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      and(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      and(fbs.source);
      //and(fbs.bits, fbs.numWords);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      fbs.advance(numBits);
    } else {
      if (numBits == 0) return;
      int disiDoc, bitSetDoc = nextSetBit(0);
      while (bitSetDoc != -1 && (disiDoc = iter.advance(bitSetDoc)) < numBits) {
        clear(bitSetDoc, disiDoc);
        disiDoc++;
        bitSetDoc = (disiDoc < numBits) ? nextSetBit(disiDoc) : -1;
      }
      if (bitSetDoc != -1) {
        clear(bitSetDoc, numBits);
      }
    }
  }

  /**
   * Due to both sets being tracked, this is very fast for all non-pathological cases.
   * @return true if the sets have any elements in common.
   **/
  public boolean intersects(TrackedFixedBitSet other) {
    final int tracker2length = Math.min(this.tracker2.length, other.tracker2.length);
    for (int tti = 0 ; tti < tracker2length ; tti++) {
      long ttBitset = this.tracker2[tti] & other.tracker2[tti];
      while (ttBitset != 0) {
        final long tt = ttBitset & -ttBitset;
        final int ti = Long.bitCount(tt-1);
        ttBitset ^= tt;

        long tBitset = this.tracker1[tti * 64 + ti] & other.tracker1[tti * 64 + ti];
        while (tBitset != 0) {
          final long t = tBitset & -tBitset;
          final int i = Long.bitCount(t-1);
          tBitset ^= t;
          if ((this.bits[tti*64*64 + ti*64 + i] & other.bits[tti*64*64 + ti*64 + i]) != 0) {
            return true;
          }
        }
      }
    }
    return false;

/*
    int pos = Math.min(numWords, other.numWords);
    while (--pos>=0) {
      if ((bits[pos] & other.bits[pos]) != 0) return true;
    }
    return false;*/
  }

  /** this = this AND other */
  public void and(TrackedFixedBitSet other) {
    merge(this, other, false, (wordNum, word1, word2) -> {
      if (word1 == 0) {
        return 0;
      }
      final long original = bits[wordNum];
      if (((bits[wordNum] = word1 & word2) == 0) && original != 0) {
        untrackWord(wordNum);
      }
      return 0;
    });
//    and(other.bits, other.numWords);
  }
  
  private void and(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits;
    int pos = Math.min(this.numWords, otherNumWords);
    while(--pos >= 0) {
      thisArr[pos] &= otherArr[pos];
    }
    if (this.numWords > otherNumWords) {
      Arrays.fill(thisArr, otherNumWords, this.numWords, 0L);
    }
  }

  /** Does in-place AND NOT of the bits provided by the
   *  iterator. */
  public void andNot(DocIdSetIterator iter) throws IOException {
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      andNot(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      andNot(fbs.source);
//      andNot(fbs.bits, fbs.numWords);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      fbs.advance(numBits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        clear(doc);
      }
    }
  }

  /** this = this AND NOT other */
  public void andNot(TrackedFixedBitSet other) {
    // Mimics the probably false behaviour of andNot(long[], int)
    merge(this, other, true, (wordNum, word1, word2) -> {
      if (word1 == 0) {
        return 0;
      }
      final long original = bits[wordNum];
      if (((bits[wordNum] = word1 & ~word2) == 0) && original != 0) {
        untrackWord(wordNum);
      }
      return 0;
    });
//    andNot(other.bits, other.bits.length);
  }
  
  private void andNot(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits; // This behavious differs from and
    int pos = Math.min(this.numWords, otherNumWords);
    while(--pos >= 0) {
      long original = thisArr[pos];
      thisArr[pos] &= ~otherArr[pos];
      if (original == 0 && thisArr[pos] != 0) {
        trackWord(pos);
      } else if (original != 0 && thisArr[pos] == 0) {
        untrackWord(pos);
      }
    }
  }

  // NOTE: no .isEmpty() here because that's trappy (ie,
  // typically isEmpty is low cost, but this one wouldn't
  // be)

  /**
   * With tracking, isEmpty is low cost: It iterates a long[] of length #bits/64/64/64..
   * @return true is no bits are set, else false.
   */
  public boolean isEmpty() {
    for (long entry : tracker2) {
      if (entry != 0) {
        return false;
      }
    }
    return true;
  }

  /** Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    /*** Grrr, java shifting wraps around so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
    long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
    long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
    ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (bits[startWord] == 0) {
      trackWord(startWord);
    } else if (bits[startWord] == -1L) {
        untrackWord(startWord);
    }

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    for (int i=startWord+1; i<endWord; i++) {
      if (bits[i] == 0) {
        trackWord(i);
      } else if (bits[i] == -1L) {
          untrackWord(i);
      }
      bits[i] = ~bits[i];
    }

    if (bits[endWord] == 0) {
      trackWord(endWord);
    } else if (bits[endWord] == -1L) {
        untrackWord(endWord);
    }
    bits[endWord] ^= endmask;
  }

  /** Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (bits[startWord] == 0) {
      trackWord(startWord);
    }

    if (startWord == endWord) {
      bits[startWord] |= (startmask & endmask);
      return;
    }

    bits[startWord] |= startmask;
    Arrays.fill(bits, startWord+1, endWord, -1L);
    trackWords(startWord + 1, endWord);

    if (bits[endWord] == 0) {
      trackWord(endWord);
    }
    bits[endWord] |= endmask;
  }

  /** Clears a range of bits.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      if (bits[startWord] != 0 && ((bits[startWord] &= (startmask | endmask)) == 0)) {
        untrackWord(startWord);
      }
      return;
    }

    if (bits[startWord] != 0 && ((bits[startWord] &= startmask) == 0)) {
      untrackWord(startWord);
    }

    Arrays.fill(bits, startWord+1, endWord, 0L);
    untrackWords(startWord + 1, endWord);

    if (bits[endWord] != 0 && ((bits[endWord] &= endmask) == 0)) {
      untrackWord(endWord);
    }
  }

  @Override
  public TrackedFixedBitSet clone() {
    long[] bits = new long[this.bits.length];
    System.arraycopy(this.bits, 0, bits, 0, bits.length);
    long[] t1 = new long[this.tracker1.length];
    System.arraycopy(this.tracker1, 0, t1, 0, t1.length);
    long[] t2 = new long[this.tracker2.length];
    System.arraycopy(this.tracker2, 0, t2, 0, t2.length);
    return new TrackedFixedBitSet(bits, numBits, t1, t2);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrackedFixedBitSet)) {
      return false;
    }
    TrackedFixedBitSet other = (TrackedFixedBitSet) o;
    if (numBits != other.length()) {
      return false;
    }
    WordIterator words1 = this.wordIterator();
    WordIterator words2 = other.wordIterator();
    int wordNum1 = words1.nextWordNum();
    int wordNum2 = words2.nextWordNum();
    while (wordNum1 != WordIterator.NO_MORE_DOCS && wordNum2 != WordIterator.NO_MORE_DOCS) {
      if (wordNum1 != wordNum2 || words1.word() != words2.word()) {
        return false;
      }
      wordNum1 = words1.nextWordNum();
      wordNum2 = words2.nextWordNum();
    }
    return wordNum1 == WordIterator.NO_MORE_DOCS && wordNum2 == WordIterator.NO_MORE_DOCS;
    //return Arrays.equals(bits, other.bits);
  }

  @Override
  public int hashCode() {
    long h = 0;
    for (int i = numWords; --i>=0;) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h>>32) ^ h) + 0x98761234;
  }
}

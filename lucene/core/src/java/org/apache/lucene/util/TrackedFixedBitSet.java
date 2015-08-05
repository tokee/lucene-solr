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
 */
public final class TrackedFixedBitSet extends DocIdSet implements Bits {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TrackedFixedBitSet.class);

  /**
   * A {@link org.apache.lucene.search.DocIdSetIterator} which iterates over set bits in a
   * {@link org.apache.lucene.util.TrackedFixedBitSet}.
   */
  public static final class TrackedFixedBitSetIterator extends DocIdSetIterator {

    final int numBits, numWords;
    final long[] t2;
    final long[] t1;
    final long[] bits;
    int doc = -1;

    /** Creates an iterator over the given {@link org.apache.lucene.util.TrackedFixedBitSet}. */
    public TrackedFixedBitSetIterator(TrackedFixedBitSet bits) {
      this(bits.bits, bits.numBits, bits.tracker1, bits.tracker2, bits.numWords);
    }

    /** Creates an iterator over the given array of bits. */
    public TrackedFixedBitSetIterator(long[] bits, int numBits, long[] t1, long[] t2, int wordLength) {
      this.bits = bits;
      this.numBits = numBits;
      this.t1 = t1;
      this.t2 = t2;
      this.numWords = wordLength;
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

  private static final class DirtyWordIterator {
    public static final int NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS;

    final int numBits, numWords;
    final long[] bits;
    final long[] t1;
    final long[] t2;
    int word = -1;
    int t1pos = 0;
    int t2pos = 0;
    long t1Bitset;
    long t2Bitset;

    public DirtyWordIterator(TrackedFixedBitSet bits) {
      this(bits.bits, bits.numBits, bits.tracker1, bits.tracker2, bits.numWords);
    }

    /** Creates an iterator over the given array of bits. */
    public DirtyWordIterator(long[] bits, int numBits, long[] t1, long[] t2, int wordLength) {
      this.bits = bits;
      this.numBits = numBits;
      this.numWords = wordLength;
      this.t1 = t1;
      this.t1Bitset = t1[0];
      this.t2 = t2;
      this.t2Bitset = t2[0];
    }

    public int nextWordNum() {
      if (word == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      throw new UnsupportedOperationException("Not implemented yet");
/*
      for (int tti = 0 ; tti < trackerTracker.length() ; tti++) {
        long ttBitset = trackerTracker.get(tti);
        while (ttBitset != 0) {
          final long tt = ttBitset & -ttBitset;
          final int ti = Long.bitCount(tt-1);
          ttBitset ^= tt;

          long tBitset = tracker.get(tti * 64 + ti);
          while (tBitset != 0) {
            final long t = tBitset & -tBitset;
            final int i = Long.bitCount(t-1);
            tBitset ^= t;
            long vBitset = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
            while (vBitset != 0) {
              final long v = vBitset & -vBitset;
              final int z = Long.bitCount(v-1);
              vBitset ^= v;

              final int counter = tti*64*64*64 + ti*64*64 + i*64 + z;
              final long value = get(counter);
              if (counter >= start && counter <= end && value >= sparseMinValue) {
                filled |= callback.handle(counter, value);
              }
            }
          }
        }
      }*/

    }

    public long word() {
      return bits[word];
    }

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
    // TODO: Copy existing tracking bits
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
    // TODO: Use tracking to optimize
    return BitUtil.pop_intersect(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
  }

  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither
   * set is modified.
   */
  public static long unionCount(TrackedFixedBitSet a, TrackedFixedBitSet b) {
    // TODO: Use tracking to optimize
    long tot = BitUtil.pop_union(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    if (a.numWords < b.numWords) {
      tot += BitUtil.pop_array(b.bits, a.numWords, b.numWords - a.numWords);
    } else if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or
   * "intersection(a, not(b))". Neither set is modified.
   */
  public static long andNotCount(TrackedFixedBitSet a, TrackedFixedBitSet b) {
    // TODO: Use tracking to optimize
    long tot = BitUtil.pop_andnot(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;
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
    return new TrackedFixedBitSetIterator(bits, numBits, tracker1, tracker2, numWords);
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
        RamUsageEstimator.sizeOf(tracker1) + RamUsageEstimator.sizeOf(tracker1);
  }

  /** Expert. */
  public long[] getBits() {
    return bits;
  }

  /** Returns number of set bits.  NOTE: this visits every not-0
   *  long in the backing bits array, and the result is not
   *  internally cached! */
  public int cardinality() {
    int cardinality = 0;
    for (int tti = 0 ; tti < tracker2.length ; tti++) {
      long ttBitset = tracker2[tti];
      while (ttBitset != 0) {
        final long tt = ttBitset & -ttBitset;
        final int ti = Long.bitCount(tt-1);
        ttBitset ^= tt;

        long tBitset = tracker1[tti * 64 + ti];
        while (tBitset != 0) {
          final long t = tBitset & -tBitset;
          final int i = Long.bitCount(t-1);
          tBitset ^= t;
          cardinality += Long.bitCount(bits[tti*64*64 + ti*64 + i]);
        }
      }
    }
    return cardinality;
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
    if ((bits[wordNum] |= bitmask) == bitmask) { // First bit in word (or duplicate, but that only harms performance)
      trackWord(wordNum);
    }
  }

  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    if ((bits[wordNum] |= bitmask) == bitmask) { // First bit in word (or duplicate, but that only harms performance)
      trackWord(wordNum);
    }
    return val;
  }

  public void clear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    if ((bits[wordNum] &= ~bitmask) == 0) {
      untrackWord(wordNum);
    }
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    if ((bits[wordNum] &= ~bitmask) == 0) {
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
    // TODO: Use trackers
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      or(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      or(fbs.bits, fbs.numWords);
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
    // TODO: Use trackers
    or(other.bits, other.numWords);
  }
  
  private void or(final long[] otherArr, final int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", otherNumWords=" + otherNumWords;
    final long[] thisArr = this.bits;
    int pos = Math.min(numWords, otherNumWords);
    while (--pos >= 0) {
    // TODO: Update trackers
      thisArr[pos] |= otherArr[pos];
    }
  }
  
  /** this = this XOR other */
  public void xor(TrackedFixedBitSet other) {
    assert other.numWords <= numWords : "numWords=" + numWords + ", other.numWords=" + other.numWords;
    final long[] thisBits = this.bits;
    final long[] otherBits = other.bits;
    int pos = Math.min(numWords, other.numWords);
    while (--pos >= 0) {
    // TODO: Update trackers
      thisBits[pos] ^= otherBits[pos];
    }
  }
  
  /** Does in-place XOR of the bits provided by the iterator. */
  public void xor(DocIdSetIterator iter) throws IOException {
    int doc;
    while ((doc = iter.nextDoc()) < numBits) {
      // TODO: Update trackers
      flip(doc, doc + 1);
    }
  }

  /** Does in-place AND of the bits provided by the
   *  iterator. */
  public void and(DocIdSetIterator iter) throws IOException {
    // TODO: Update trackers
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      and(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      and(fbs.bits, fbs.numWords);
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

  /** returns true if the sets have any elements in common */
  public boolean intersects(TrackedFixedBitSet other) {
    // TODO: Use trackers
    int pos = Math.min(numWords, other.numWords);
    while (--pos>=0) {
      if ((bits[pos] & other.bits[pos]) != 0) return true;
    }
    return false;
  }

  /** this = this AND other */
  public void and(TrackedFixedBitSet other) {
    // TODO: Use trackers
    and(other.bits, other.numWords);
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
    // TODO: Use trackers
    if (iter instanceof OpenBitSetIterator && iter.docID() == -1) {
      final OpenBitSetIterator obs = (OpenBitSetIterator) iter;
      andNot(obs.arr, obs.words);
      // advance after last doc that would be accepted if standard
      // iteration is used (to exhaust it):
      obs.advance(numBits);
    } else if (iter instanceof TrackedFixedBitSetIterator && iter.docID() == -1) {
      final TrackedFixedBitSetIterator fbs = (TrackedFixedBitSetIterator) iter;
      andNot(fbs.bits, fbs.numWords);
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
    // TODO: Use trackers
    andNot(other.bits, other.bits.length);
  }
  
  private void andNot(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits;
    int pos = Math.min(this.numWords, otherNumWords);
    while(--pos >= 0) {
      thisArr[pos] &= ~otherArr[pos];
    }
  }

  // NOTE: no .isEmpty() here because that's trappy (ie,
  // typically isEmpty is low cost, but this one wouldn't
  // be)

  /** Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    // TODO: Update trackers
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

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    for (int i=startWord+1; i<endWord; i++) {
      bits[i] = ~bits[i];
    }

    bits[endWord] ^= endmask;
  }

  /** Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    // TODO: Update trackers
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
    return Arrays.equals(bits, other.bits);
  }

  @Override
  public int hashCode() {
    // TODO: Use trackers
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

/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.solr.search.sparse;

import java.util.Arrays;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.count.plane.NPlaneMutable;

/**
 * An iterator structure that delivers the number of bits needed to hold the source values, represented at integers.
 * Used by some of the sparse counters, notably {@link NPlaneMutable}, during initialization.
 *
 * Sample: If the maximum bucket sizes (term instance count) for a facet field are [1, 4, 0, 3], the delivered
 * BPV's will be [1, 3, 0, 2].
 */
public interface BPVProvider {
  /**
   * @return true if there is more BPVs available.
   */
  boolean hasNext();

  /**
   * Calling this increments the iterator.
   * @return the bits required for the next source value.
   */
  int nextBPV();

  /**
   * Calling this increments the iterator.
   * @return the next source value.
   */
  long nextValue();

  /**
   * Reset the structure for another iteration.
   */
  void reset();

  /**
   * Wraps a {@link BPVProvider} and provided the new method {@link #nextZeroBPV()} that is identical to
   * {@link #nextBPV()} with the single difference that a source value og 0 will result in 1 bpv (instad of 0).
   */
  class BPVZeroWrapper implements BPVProvider {
    private final BPVProvider source;

    public BPVZeroWrapper(BPVProvider source) {
      this.source = source;
    }

    /**
     * @return 1 if {@link #nextValue()} == 0, else {@link #nextBPV()}.
     */
    public int nextZeroBPV() {
      final long ordCount = (int) source.nextValue();
      return ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public int nextBPV() {
      return source.nextBPV();
    }

    @Override
    public long nextValue() {
      return source.nextValue();
    }

    @Override
    public void reset() {
      source.reset();
    }
  }

  /**
   * Updates a histogram of BPVs required during iteration.
   */
  class StatCollectingBPVWrapper implements BPVProvider {
    private final BPVProvider source;

    public long entries = 0;
    public long maxCount = -1;
    public long refCount = 0;
    public final long[] histogram = new long[64];
    /*
    The plusOneHistogram is used by {@link NPlaneMutable} for creating an instance optimized for cheap
    checks for counter values different from 0.
    For details, see https://sbdevel.wordpress.com/2015/04/30/alternative-counter-tracking/
    Decimal  0  1   2    3    4     5     6     7     8
    Binary   0  1  10   11  100   101   110   111  1000
    Binary+  0  1  11  101  111  1001  1011  1101  1111
     */
    public final long[] plusOneHistogram = new long[64];

    public StatCollectingBPVWrapper(BPVProvider source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public int nextBPV() {
      final long ordCount = (int) source.nextValue();
      final int bpv = PackedInts.bitsRequired(ordCount);
      { // Statistics collection
        histogram[bpv]++;
        plusOneHistogram[ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return bpv;
    }
    @Override
    public long nextValue() {
      final long ordCount = (int) source.nextValue();
      { // Statistics collection
        final int bpv = PackedInts.bitsRequired(ordCount);
        histogram[bpv]++;
        plusOneHistogram[ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return ordCount;
    }

    @Override
    public void reset() {
      source.reset();
      entries = 0;
      maxCount = -1;
      refCount = 0;
      Arrays.fill(histogram, 0);
      Arrays.fill(plusOneHistogram, 0);
    }

    /**
     * Convenience method if the only purpose is to collect statistics.
     */
    public void collect() {
      while (hasNext()) {
        nextValue();
      }
    }

    @Override
    public String toString() {
      return "StatCollectingBPVWrapper(" +
          "entries=" + entries + ", maxCount=" + maxCount + ", refCount=" + refCount +
          ", histogram=" + Arrays.toString(histogram) + ", plusOneHistogram=" + Arrays.toString(plusOneHistogram) + ')';
    }
  }

  /**
   * Exposes a given {@code int[]} as a BPVProvider.
   */
  class BPVIntArrayWrapper implements BPVProvider {
    private final int[] maxima;
    private final boolean alreadyBPV;
    private int pos = 0;

    public BPVIntArrayWrapper(int[] maxima, boolean alreadyBPV) {
      this.maxima = maxima;
      this.alreadyBPV = alreadyBPV;
    }

    public int size() {
      return maxima.length;
    }

    @Override
    public boolean hasNext() {
      return pos < maxima.length;
    }

    @Override
    public int nextBPV() {
      return alreadyBPV ? maxima[pos++] : PackedInts.bitsRequired(maxima[pos++]);
    }

    @Override
    public long nextValue() {
      if (alreadyBPV) {
        throw new UnsupportedOperationException("The maxima are represented as BPV internally. The raw value is lost");
      }
      return maxima[pos++];
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  /**
   * Exposes a given {@link PackedInts.Reader} as a BPVProvider.
   */
  class BPVPackedWrapper implements BPVProvider {
    private final PackedInts.Reader maxima;
    private final boolean alreadyBPV;
    private int pos = 0;

    public BPVPackedWrapper(PackedInts.Reader maxima, boolean alreadyBPV) {
      this.maxima = maxima;
      this.alreadyBPV = alreadyBPV;
    }

    public int size() {
      return maxima.size();
    }

    @Override
    public boolean hasNext() {
      return pos < maxima.size();
    }

    @Override
    public int nextBPV() {
      return alreadyBPV ? (int) maxima.get(pos++) : PackedInts.bitsRequired(maxima.get(pos++));
    }

    @Override
    public long nextValue() {
      if (alreadyBPV) {
        throw new UnsupportedOperationException("The maxima are represented as BPV internally. The raw value is lost");
      }
      return maxima.get(pos++);
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  /**
   * An extensible collection of BPVs, where the collected BPVs can be retrieved as a {@link BPVProvider},
   */
  class BPVAbsorber {
    private final PackedInts.Mutable bpvs;
    private int pos = 0;
    public BPVAbsorber(int size, int maxBPV) {
      bpvs = PackedInts.getMutable(size, maxBPV, PackedInts.COMPACT);
    }
    public void addBPV(int bpv) {
      bpvs.set(pos++, bpv);
    }
    public void addAbsolute(long maxValue) {
      bpvs.set(pos++, PackedInts.bitsRequired(maxValue));
    }
    public BPVProvider getProvider() {
      return new BPVPackedWrapper(bpvs, true);
    }
  }
}



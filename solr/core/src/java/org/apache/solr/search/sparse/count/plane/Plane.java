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

package org.apache.solr.search.sparse.count.plane;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.count.Incrementable;

/**
 * Helper class for {@link NPlaneMutable}.
 *
 * The bits needed for a counter are divided among 1 or more planes.
 * Each plane holds n bits, where n > 0.
 * Besides the bits themselves, each plane (except the last) holds 1 bit for each value,
 * signalling if the value overflows onto the nextBPV plane.
 *
 * Example:
 * There are 5 counters, where the max for the first counter is 10, the max for the second
 * counter is 1 and the maxima for the rest are 16, 2 and 3. The bits needed to hold the
 * first counter is 4, since {@code 2^3-1 < 10 < 2^4-1}. The bits needed for the rest of
 * the values are 1, 5, 2 and 2.<br/>
 * plane(0) holds the 2 least significant bits (bits 0+1), plane(1) holds bits 2+3, plane(2) holds bit 4.<br/>
 * plane(0) holds 5 * 2 value bits + 5 * 1 overflow-bit.<br/>
 * plane(1) holds 2 * 1 value bits + 2 * 1 overflow-bit.<br/>
 * plane(2) holds 1 * 1 value bits and no overflow bits, since it is the last one.<br/>
 * Stepping through the maxima-bits 4, 1, 5, 2 and 2, we have<br/>
 * plane(0).overflowBits = 1 0 1 0 0<br/>
 * plane(1).overflowBits = 0 1
 */
public abstract class Plane {
  /**
   * The number of valid buckets (aka values) in this counter.
   */
  public final int valueCount;
  /**
   * The number of bits used for each value in this plane.
   */
  public final int bpv;
  /**
   * The sum of bits used for each value in this counter and in the previous counters.
   */
  public final int maxBit; // Max up to this point
  /**
   * If true, there are further planes and values in this plane can overflow into those.
   */
  public final boolean hasOverflow;

  /**
   *
   * @param valueCount  the number of values (buckets) for this plane.
   * @param bpv         the number of bits used for each value in this plane.
   * @param maxBit      the sum of bits used for each value in this counter and in the previous counters.
   * @param hasOverflow if true, there are further planes and values in this plane can overflow into those.
   */
  public Plane(int valueCount, int bpv, int maxBit, boolean hasOverflow) {
    this.valueCount = valueCount;
    this.bpv = bpv;
    this.maxBit = maxBit;
    this.hasOverflow = hasOverflow;
  }

  /**
   * Increment the value at the given index by 1.

   * @param index the value to increment.
   * @return semi-detailed info about the operation.
   */
  public abstract Incrementable.STATUS inc(int index);

  /**
   * Set the value at the given index. Max of 2^bpv-1 must be ensured by the caller.
   * Calling with values less than 0 or more than the max has undefined behaviour.
   */
  public abstract void set(int index, long value);

  /**
   * @param extraInstance if true, this plane is treated as an extra plane in the calculation of memory used.
   *                      The effect is that only the non-shared elements (the mutable bits) are used for calculation.
   * @return the number of bytes needed for this plane.
   */
  public abstract long ramBytesUsed(boolean extraInstance);

  /**
   * @param index relative to the conceptual overflow bits.
   * @return the number of set overflow bits up to but not including the given index.
   */
  public abstract int overflowRank(int index);

  /**
   * Mark the bucket at the given index as overflowing up to the next plane.
   * Overflow in this context means that there are more significant bits, not that the concrete content of the
   * bucket has overflowed. Overflows will be set once during initialization of the plane, after which
   * {@link #finalizeOverflow()} will be called.
   * @param index the index for the bucket that can overflow up to the next plane.
   */
  public abstract void setOverflow(int index);

  /**
   * @return true if the bucket at the given index can overflow to the next plane.
   */
  public abstract boolean isOverflow(int index);

  /**
   * Called after all overflow-flags has been set with {@link #setOverflow(int)}. Normally this will create a rank
   * structure (see {@link RankCache}).
   */
  public abstract void finalizeOverflow();

  public long ramBytesUsed() {
    return ramBytesUsed(false);
  }

  /**
   * Clear all buckets on the plane.
   */
  public abstract void clear();

  /**
   * @return the value at the given index.
   */
  public abstract long get(int index);

  public String toString() {
    return "Plane(#values=" + valueCount + ", bpv=" + bpv + ", maxBit=" + maxBit + ", overflow=" + hasOverflow + ")";
  }

  /**
   * Creates a new Plane that shares overflow structures with the current Plane.
   * Used to get multiple counters with the same maxima.
   * The counter bits for the constructed Plane will be empty and ready for use.
   * @return a Plane sharing components with this Plane, but with independent counter values.
   */
  public abstract Plane createSibling();

  /**
   * Returns a long representing 64 values, with each bit being a flag for one value.
   * If the flag for a value is 1, the corresponding value across all planes is at least 1.
   * @param longIndex value-index / 64.
   * @return 64 flags for non-zero.
   */
  public abstract long getNonZeroBits(int longIndex);

  /**
   * A light weight representation of a plane, without structures for counter data. Used for estimations.
   */
  public static class PseudoPlane {
    private final int valueCount;
    private final int bpv;
    private final boolean hasOverflows;
    private final int maxBit;
    private final int overflowBucketSize;

    public PseudoPlane(int valueCount, int bpv, boolean hasOverflows, int overflowBucketSize, int maxBit) {
      this.valueCount = valueCount;
      this.bpv = bpv;
      this.hasOverflows = hasOverflows;
      this.maxBit = maxBit;
      this.overflowBucketSize = overflowBucketSize;
    }

    /**
     * Creates a Plane from the data given during construction of the pseudo plane and the requested implementation.
     * The plane needs to be further processed with regards to overflow before it is usable.
     */
    public Plane createPlane(NPlaneMutable.IMPL impl) {
      switch (impl) {
        case split:  return new SplitPlane(valueCount, bpv, hasOverflows, overflowBucketSize, maxBit);
        case spank:  return new SplitRankPlane(valueCount, bpv, hasOverflows, maxBit, false);
        case tank:   return new SplitRankPlane(valueCount, bpv, hasOverflows, maxBit, true);
        case zethra: return new SplitRankZeroPlane(valueCount, bpv, hasOverflows, maxBit, true);
        case shift:  return new ShiftPlane(valueCount, bpv, hasOverflows, overflowBucketSize, maxBit);
        default: throw new UnsupportedOperationException("No Plane implementation available for " + impl);
      }
    }

    public long estimateBytesNeeded(boolean extraInstance, NPlaneMutable.IMPL impl) {
      switch (impl) {
        case split: {
          long bytes = RamUsageEstimator.alignObjectSize(
              3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES) + // Plane object
              RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 1L*valueCount*bpv/8; // Values, assuming compact
          if (!extraInstance) {
            bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER; // overflow header
            if (hasOverflows) {
              bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 1L*valueCount/8 + // overflow bits
                  valueCount/overflowBucketSize*PackedInts.bitsRequired(valueCount)/8; // Cache
            }
          }
          return bytes;
        }
        case spank:
        case zethra:
        case tank: {
          long bytes = RamUsageEstimator.alignObjectSize(
              3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) + // Plane object
              RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 1L*valueCount*bpv/8; // Values, assuming compact
          if (!extraInstance) {
            bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER; // overflow header
            if (hasOverflows) {
              bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + valueCount/8 + // overflow bits
                  ((long)valueCount)*64/2048; // Rank cache uses a long for every 2048 bits
            }
          }
          return bytes;
        }
        case shift: return -1;
        // TODO: Add shift
        default: throw new UnsupportedOperationException("No memory estimation available for " + impl);
      }
    }

    public String toString() {
      return String.format("PseudoPlane(#values=%9d, bpv=%2d, overflows=%5b, maxBits=%2d)",
          valueCount, bpv, hasOverflows, maxBit);
    }
  }

  private static int MAX_PRINT = 20;

  static void toString(StringBuilder sb, PackedInts.Mutable values, int bit) {
    for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
      sb.append((values.get(i) >> bit) & 1);
    }
  }

  static void toString(StringBuilder sb, PackedInts.Mutable values) {
    for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
      sb.append(values.get(i)).append(" ");
    }
  }

  static void toString(StringBuilder sb, FixedBitSet overflow) {
    for (int i = 0; i < MAX_PRINT && i < overflow.length(); i++) {
      sb.append(overflow.get(i) ? "*" : "-");
    }
  }

}

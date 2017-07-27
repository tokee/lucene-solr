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
package org.apache.solr.search.sparse.count.plane;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.count.Incrementable;

/**
 * Stores overflow-flags as bit-0 in the buckets, thereby requiring only a single memory lookup to get both the current
 * value and isOverflowing. For a single instance, this has the samme memory requirements as storing value-bits and
 * overflow-bits in separate structures. For extra instances, the overflow-bits cannot be shared and thus impose
 * extra memory overhead.
 *
 * The rank-implementation for this plane is a simple list of the collective rank for previous chunks, requiring
 * further iterative lookups to determine the exact rank for a bucket.
 *
 * Due to the non-reusability of the static data (the overflow bits) and the slow rank function, this implementation
 * is not recommended. Even with improved ranking, it is doubtful that the saved memory lookup for the overflow bit
 * is worth it.
 *
 * This implementation is not Thread safe.
 * @deprecated as the trade-offs in this implementation are poor, compared to the alternative {@link Plane}s.
 *             Use {@link SplitRankPlane} or {@link SplitRankZeroPlane} instead.
 */
public class ShiftPlane extends Plane {
  private final PackedInts.Mutable values;
  private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
  private final int overflowBucketSize;
  private final long VALUE_BITS_MASK;

  public ShiftPlane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
    super(valueCount, bpv, maxBit, hasOverflow);
    values = PackedInts.getMutable(valueCount, hasOverflow ? bpv + 1 : bpv, PackedInts.COMPACT);
    this.overflowBucketSize = overflowBucketSize;
    overflowCache = PackedInts.getMutable( // TODO: Spare the +1
        valueCount / overflowBucketSize + 1, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
    VALUE_BITS_MASK = ~(~1L << (bpv - 1));
  }

  private ShiftPlane(PackedInts.Mutable values, PackedInts.Mutable overflowCache, boolean hasOverflow,
                     int overflowBucketSize, int maxBit, int bpv) {
    super(values.size(), bpv, maxBit, hasOverflow);
    this.values = values;
    this.overflowCache = overflowCache;
    this.overflowBucketSize = overflowBucketSize;
    VALUE_BITS_MASK = ~(~1L << (bpv - 1));
  }

  // In reality this shares only the overflowCache with siblings, not the overflow bits
  @Override
  public Plane createSibling() {
    PackedInts.Mutable emptyValues = NPlaneMutable.newFromTemplate(values);
    if (hasOverflow) { // Copy the overflow bits
      for (int i = 0; i < valueCount; i++) {
        emptyValues.set(i, values.get(i) & 0x1);
      }
    }
    return new ShiftPlane(emptyValues, overflowCache, false, overflowBucketSize, maxBit, bpv);
  }

  @Override
  public Incrementable.STATUS inc(int index) {
    long value;
    if (hasOverflow) {
      final long rawValue = values.get(index);
      value = rawValue >>> 1;
      value++;
      values.set(index, ((value & VALUE_BITS_MASK) << 1) | (rawValue & 0x1));
    } else {
      value = values.get(index) + 1;
      values.set(index, value & VALUE_BITS_MASK);
    }
    return value == 1 ? Incrementable.STATUS.wasZero :
        (value >>> bpv) != 0 ? Incrementable.STATUS.overflowed : Incrementable.STATUS.ok;
  }

  @Override
  public void set(int index, long value) {
    if (hasOverflow) {
      values.set(index, (value << 1) | (values.get(index) & 0x1));
    } else {
      values.set(index, value);
    }
  }

  @Override
  public boolean isOverflow(int index) {
    return (values.get(index) & 0x1) == 1;
  }

  // Nukes existing values
  @Override
  public void setOverflow(int index) {
    values.set(index, 1);
  }

  @Override
  public void finalizeOverflow() {
    if (!hasOverflow) {
      return;
    }
    for (int i = 0; i < valueCount; i++) {
      final int cacheIndex = i / overflowBucketSize;
      if (cacheIndex > 0 && i % overflowBucketSize == 0) { // Over the edge
        overflowCache.set(cacheIndex, overflowCache.get(cacheIndex - 1)); // Transfer previous sum
      }

      if (!isOverflow(i)) {
        continue;
      }
      overflowCache.set(cacheIndex, overflowCache.get(cacheIndex) + 1);
    }
  }

  // Quite heavy as we cannot use Arrays.fill underneath
  @Override
  public void clear() {
    if (!hasOverflow) {
      values.clear();
      return;
    }
    for (int i = 0; i < valueCount; i++) {
      values.set(i, values.get(i) & 0x1);
    }
  }

  @Override
  public long get(int index) {
    return hasOverflow ? values.get(index) >>> 1 : values.get(index);
  }

  @Override
  public long ramBytesUsed(boolean extraInstance) {
    return RamUsageEstimator.alignObjectSize(2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES) +
        values.ramBytesUsed() + overflowCache.ramBytesUsed();
  }

  // Using the overflow and overflowCache, calculate the index into the nextBPV plane
  @Override
  public int overflowRank(int index) {
    int startIndex = 0;
    int rank = 0;
    if (index >= overflowBucketSize) {
      rank = (int) overflowCache.get(index / overflowBucketSize - 1);
      startIndex = index / overflowBucketSize * overflowBucketSize;
    }
    // It would be nice to use cardinality in this situation, but that only works on the full bitset(?)
    for (int i = startIndex; i < index; i++) {
      if (isOverflow(i)) {
        rank++;
      }
    }
    return rank;
  }

  @Override
  public long getNonZeroBits(int longIndex) {
    throw new UnsupportedOperationException("Non-zero bits are not fast to calculate for this implementation");
  }

  public String toString() {
    return "ShiftPlane(" + super.toString() + ")";
  }
}

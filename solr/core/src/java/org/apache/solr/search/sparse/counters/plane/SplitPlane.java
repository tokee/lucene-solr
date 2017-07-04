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
package org.apache.solr.search.sparse.counters.plane;

import java.util.Locale;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.counters.Incrementable;

/**
 * Stores the static overflow-bits in a bit set separate from the value bits, sharing this static structure between
 * plane instances.
 *
 * The rank-implementation for this plane is a simple list of the collective rank for previous chunks, requiring
 * further iterative lookups to determine the exact rank for a bucket. This makes overall value incrementation slow
 * and should be improved, if this implementations is to be a serious contender.
 *
 * This implementation is not Thread safe.
 * @deprecated due to the slow rank function. Use {@link SplitRankPlane} or {@link SplitRankZeroPlane} instead.
 */
public class SplitPlane extends Plane {
  private final PackedInts.Mutable values;     // TODO: Make an incrementable version
  private final FixedBitSet overflows;
  private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
  private final int overflowBucketSize;

  public SplitPlane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
    super(valueCount, bpv, maxBit, hasOverflow);
    values = PackedInts.getMutable(valueCount, bpv, PackedInts.COMPACT);
    overflows = new FixedBitSet(hasOverflow ? valueCount : 0);
    this.overflowBucketSize = overflowBucketSize;
    overflowCache = PackedInts.getMutable( // TODO: Spare the +1
        valueCount / overflowBucketSize + 1, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
  }

  private SplitPlane(int valueCount, int bpv, int maxBit, boolean hasOverflow, PackedInts.Mutable values,
                     FixedBitSet overflows, PackedInts.Mutable overflowCache, int overflowBucketSize) {
    super(valueCount, bpv, maxBit, hasOverflow);
    this.values = values;
    this.overflows = overflows;
    this.overflowCache = overflowCache;
    this.overflowBucketSize = overflowBucketSize;
  }

  @Override
  public Plane createSibling() {
    return new org.apache.solr.search.sparse.counters.plane.SplitPlane(
        valueCount, bpv, maxBit, hasOverflow, NPlaneMutable.newFromTemplate(values),
        overflows, overflowCache, overflowBucketSize);
  }

  @Override
  public Incrementable.STATUS inc(int index) {
    long value = values.get(index);
    value++;
    values.set(index, value & ~(~1L << (values.getBitsPerValue() - 1)));
    return value == 1 ? Incrementable.STATUS.wasZero :
        (value >>> values.getBitsPerValue()) != 0 ? Incrementable.STATUS.overflowed : Incrementable.STATUS.ok;
  }

  @Override
  public void set(int index, long value) {
    values.set(index, value);
  }

  @Override
  public boolean isOverflow(int index) {
    return overflows.get(index);
  }

  @Override
  public void setOverflow(int index) {
    overflows.set(index);
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

      if (!overflows.get(i)) {
        continue;
      }
      overflowCache.set(cacheIndex, overflowCache.get(cacheIndex) + 1);
    }
  }

  @Override
  public void clear() {
    values.clear();
  }

  @Override
  public long get(int index) {
    return values.get(index);
  }

  @Override
  public long ramBytesUsed(boolean extraInstance) {
    long bytes = RamUsageEstimator.alignObjectSize(
        3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES) + values.ramBytesUsed();
    if (!extraInstance) {
      bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + overflows.length() / 8 + overflowCache.ramBytesUsed();
    }
    return bytes;
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
      if (overflows.get(i)) {
        rank++;
      }
    }
    return rank;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < values.getBitsPerValue(); i++) {
      sb.append(String.format(Locale.ENGLISH, "Values(%2d): ", maxBit - values.getBitsPerValue() + i));
      toString(sb, values, i);
      sb.append("\n");
    }
    sb.append("Overflow:   ");
    toString(sb, overflows);
    sb.append(" cache: ");
    toString(sb, overflowCache);
    return sb.toString();
  }

  @Override
  public long getNonZeroBits(int longIndex) {
    throw new UnsupportedOperationException("Non-zero bits are not fast to calculate for this implementation");
  }
}

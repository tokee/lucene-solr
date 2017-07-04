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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.counters.Incrementable;
import org.apache.solr.search.sparse.counters.PackedOpportunistic;

/**
 * Specialization of {@link SplitRankPlane} that exposes zero-tracking if maxBit is 1.
 * If it is not zero-tracked, it behaves as a standard SplitRankPlane.
 *
 * If this is zero-tracked (and thus with 1 bpv), incrementing always leaves the value-bit set to 1, even if it was
 * already 1. This provides fast 64-bit-blocks checks for changed values using {@link #getNonZeroBits(int)}.
 *
 * Semi-thread-safe (concurrent calls to {@link #inc(int)} are safe).
 * // TODO: Create a non-thread-safe version for (assumedly) better performance
 */
public class SplitRankZeroPlane extends SplitRankPlane {
  protected final PackedOpportunistic.PackedOpportunistic1 zeroTracker; // Only defined for first plane

  public SplitRankZeroPlane(int valueCount, int bpv, boolean hasOverflow, int maxBit, boolean threadGuarded) {
    // The +64 is a hack to avoid overflow with bitmap tracking. The cause for the overflow has yet to be determined
    super(valueCount, bpv, hasOverflow, maxBit, threadGuarded);
    zeroTracker = (values instanceof PackedOpportunistic.PackedOpportunistic1) && maxBit == 1 ?
        (PackedOpportunistic.PackedOpportunistic1) values : null;
  }

  private SplitRankZeroPlane(
      PackedInts.Mutable values, FixedBitSet overflows, int maxBit, boolean hasOverflow, int bpv) {
    super(values, overflows, maxBit, hasOverflow, bpv);
    zeroTracker = (values instanceof PackedOpportunistic.PackedOpportunistic1) && maxBit == 1 ?
        (PackedOpportunistic.PackedOpportunistic1) values : null;
  }

  @Override
  public Incrementable.STATUS inc(int index) {
    return zeroTracker == null ? incValues.incrementStatus(index) : zeroTracker.incrementCeilStatus(index);
  }

  @Override
  public Plane createSibling() {
    return new SplitRankZeroPlane(NPlaneMutable.newFromTemplate(values), overflows, maxBit, hasOverflow, bpv);
  }

  // Always produces a thread-safe PackedOpportunistic
  @Override
  protected PackedInts.Mutable getPacked(int valueCount, int bpv, boolean threadGuarded) {
    // Always use the thread safe as that is the only one that has a zeroTracked inner counter
    for (int candidateBPV = bpv; candidateBPV < 64; candidateBPV++) {
      if (PackedOpportunistic.isSupported(candidateBPV)) {
        return PackedOpportunistic.create(valueCount, candidateBPV);
      }
    }
    throw new IllegalArgumentException("Unable to create PackedOpportunistic with BitsPerValue=" + bpv);
  }

  @Override
  public long getNonZeroBits(int longIndex) {
    if (zeroTracker == null) {
      throw new UnsupportedOperationException(
          "Non-zero bits can only be returned for plane 1 with a zeroTracker");
    }
    return zeroTracker.getBlock(longIndex);
  }
}

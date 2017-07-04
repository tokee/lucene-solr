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
import org.apache.solr.search.sparse.counters.PackedOpportunistic;

/**
 * Separate storage of value bits and overflow bits, where overflow bits are shared across instances.
 * Efficient rank function with {@link RankCache}.
 *
 * Semi-thread-safe (concurrent calls to {@link #inc(int)} are safe) if specified upon construction.
 *
 * This plane is normally used with {@link SplitRankZeroPlane}.
 */
public class SplitRankPlane extends Plane {
  protected final PackedInts.Mutable values;
  protected final Incrementable incValues;
  protected final RankCache rankCache;
  protected final FixedBitSet overflows;

  public SplitRankPlane(int valueCount, int bpv, boolean hasOverflow, int maxBit, boolean threadGuarded) {
    super(valueCount, bpv, maxBit, hasOverflow);
    values = getPacked(valueCount, bpv, threadGuarded);
    incValues = values instanceof Incrementable ? (Incrementable)values : new Incrementable.IncrementableMutable(values);
    overflows = new FixedBitSet(hasOverflow ? valueCount : 0);
    rankCache = new RankCache(overflows, false);
  }

  SplitRankPlane(PackedInts.Mutable values, FixedBitSet overflows, int maxBit, boolean hasOverflow, int bpv) {
    super(values.size(), bpv, maxBit, hasOverflow);
    this.values = values;
    incValues = values instanceof Incrementable ? (Incrementable)values : new Incrementable.IncrementableMutable(values);
    this.overflows = overflows;
    rankCache = new RankCache(overflows, false);
  }

  @Override
  public Plane createSibling() {
    return new SplitRankPlane(NPlaneMutable.newFromTemplate(values), overflows, maxBit, hasOverflow, bpv);
  }
  protected PackedInts.Mutable getPacked(int valueCount, int bpv, boolean threadGuarded) {
    if (!threadGuarded) {
      return PackedInts.getMutable(valueCount, bpv, PackedInts.COMPACT);
    }

    for (int candidateBPV = bpv; candidateBPV < 64 ; candidateBPV++) {
      if (PackedOpportunistic.isSupported(candidateBPV)) {
        return PackedOpportunistic.create(valueCount, candidateBPV);
      }
    }
    throw new IllegalArgumentException("Unable to create PackedOpportunistic with BitsPerValue="  + bpv);
  }

  @Override
  public Incrementable.STATUS inc(int index) {
    return incValues.incrementStatus(index);
//      return (incValues.incrementStatus(index) >>> values.getBitsPerValue()) != 0;
/*      long value = values.get(index);
    value++;
    values.set(index, value & ~(~1L << (values.getBitsPerValue()-1)));
    return (value >>> values.getBitsPerValue()) != 0;
*/
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
    if (hasOverflow) {
      rankCache.buildRankCache();
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
      bytes += overflows.ramBytesUsed();
    }
    return bytes;
  }

  @Override
  public int overflowRank(int index) {
    if (overflows != null) {
      try {
        return rankCache.rank(index);
      } catch (NullPointerException e) {
        throw new NullPointerException("Exceeded overflow bits for plane " + this.getClass().getSimpleName() + " with"
            + " maxBit=" + maxBit + " and #overflow-bits=" + overflows.length() + " when requesting index " + index);
      }
    }
    throw new NullPointerException("No overflow bits for plane " + this.getClass().getSimpleName()
        + "with maxBit=" + maxBit);
  }

  @Override
  public long getNonZeroBits(int longIndex) {
      throw new UnsupportedOperationException("Non-zero bits can only be returned for plane 1 with a zeroTracker");
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
    return sb.toString();
  }
}

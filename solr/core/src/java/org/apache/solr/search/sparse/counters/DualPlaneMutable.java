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

package org.apache.solr.search.sparse.counters;

import org.apache.lucene.util.packed.PackedInts;

/**
 * Holds large values in {@link #head} and small values in {@link #tail}, thereby reducing overall memory
 * consumption, compared to Packed64. Performance overhead, compared to Packed64, is very small
 * and depending on the concrete values.
 * </p><p>
 * The reduced memory consumption is possible if the value distribution is an unordered long tail.
 * Ordered long tail would allow for markedly lower memory consumption, but would imply that all values
 * appeared in sorted order.
 */
public class DualPlaneMutable extends PackedInts.Mutable implements Incrementable {
  private static final double DEFAULT_MAXSIZEFRACTION = 100.0; // Effectively disabled
  private final PackedInts.Mutable head;
  private final int headBit;
  private int headPos = 0;
  private final PackedInts.Mutable tail;
  private final int tailValueMask;

  public static long totalCounters(long[] histogram) {
    long total = 0;
    for (int i = 0; i < histogram.length; i++) {
      total += histogram[i];
      //total += histogram[i]*(i+1); // TODO: Check if we need the factor by exhaustive inc-test on a small sample
    }
    return total;
  }

  public PackedInts.Mutable createEmpty(PackedInts.Reader maxCounts, double maxSizeFraction) {
    return create(maxCounts.size(), getHistogram(maxCounts), maxSizeFraction);
  }

  public static long[] getHistogram(PackedInts.Reader maxCounts) {
    final long[] histogram = new long[64];
    for (int i = 0 ; i < maxCounts.size() ; i++) {
      histogram[PackedInts.bitsRequired(maxCounts.get(i))]++;
    }
    return histogram;
  }

  public PackedInts.Reader compress(PackedInts.Reader values, double maxSizeFraction) {
    PackedInts.Mutable mutable = createEmpty(values, maxSizeFraction);
    if (mutable == null) {
      return null;
    }
    for (int i = 0 ; i< values.size() ; i++) {
      this.set(i, values.get(i));
    }
    return mutable;
  }

  public static DualPlaneMutable create(long[] histogram) {
    return create((int) totalCounters(histogram), histogram, DEFAULT_MAXSIZEFRACTION);
  }
  public static DualPlaneMutable create(long[] histogram, double maxSizeFraction) {
    return create((int) totalCounters(histogram), histogram, maxSizeFraction);
  }
  public static DualPlaneMutable create(int valueCount, long[] histogram, double maxSizeFraction) {
    if (histogram.length != 64) {
      throw new IllegalArgumentException("The histogram length must be exactly 64, but it was " + histogram.length);
    }

    Estimate estimate = new Estimate(valueCount, histogram);
    int tailBPV = estimate.getMostCompactTailBPV();
    if (tailBPV == 0 || estimate.getFractionEstimate(tailBPV) > maxSizeFraction) {
      return null; // No viable candidate
    }

    return new DualPlaneMutable((int) estimate.getHeadValueCount(tailBPV), valueCount, estimate.getMaxBPV(), tailBPV);
  }

  public static long estimateBytesNeeded(long[] histogram, int valueCount) {
    Estimate estimate = new Estimate(valueCount, histogram);
    int tailBPV = estimate.getMostCompactTailBPV();
    return estimate.getMemEstimate(tailBPV);
  }

  public DualPlaneMutable(int headValueCount, int tailValueCount, int headBPV, int tailBPV) {
    head = PackedInts.getMutable(headValueCount, headBPV, PackedInts.FASTEST); // There should not be many values
    headBit = 1 << tailBPV;
    tail = PackedInts.getMutable(tailValueCount, tailBPV+1, PackedInts.DEFAULT);
    tailValueMask = headBit - 1;
  }

  public static class Estimate {
    private final long valueCount;
    private final long[] histogram;
    private final int maxBPV;
    private final long[] estimatedMem = new long[64]; // index = tailBPV, 0 = non-viable

    public Estimate(PackedInts.Reader maxValues) {
      this(maxValues.size(), createHistogram(maxValues));
    }
    public Estimate(long valueCount, long[] histogram) {
      if (histogram.length != 64) {
        throw new IllegalArgumentException("The histogram length must be exactly 64, but it was " + histogram.length);
      }
      this.valueCount = valueCount;
      this.histogram = histogram;
      int maxBPV = 0;
      for (int tailBPV = 1 ; tailBPV < 64 ; tailBPV++) {
        fillMemAndHead(tailBPV);
        if (histogram[tailBPV] != 0) {
          maxBPV = tailBPV+1; // bits count from 0 and we need the total amounts of bits
        }
      }
      this.maxBPV = maxBPV;
    }

    private void fillMemAndHead(int tailBPV) {
      long valuesAboveTailBPV = getHeadValueCount(tailBPV);
      if (valuesAboveTailBPV == 0) { // Only tail
        estimatedMem[tailBPV] = valueCount * tailBPV / 8;
        return;
      }

      long headMaxCount = (int) Math.pow(2, tailBPV);
      if (valuesAboveTailBPV <= headMaxCount) { // tail + fastHead
        estimatedMem[tailBPV] = valuesAboveTailBPV * maxBPV / 8 + valueCount * (tailBPV + 1) / 8;
        return;
      }
      estimatedMem[tailBPV] = 0; // Unviable
    }

    /* The amount of values in head that cannot fit as direct pointers in tail
     */
    public long getHeadValueCount(int tailBPV) {
      long valuesAboveTailBPV = 0;
      for (int bpv = 64 ; bpv > tailBPV ; bpv--) {
        valuesAboveTailBPV += histogram[bpv-1];
      }
      return valuesAboveTailBPV;
    }

    private static long[] createHistogram(PackedInts.Reader maxValues) {
      long[] histogram = new long[64];
      for (int i = 0 ; i < maxValues.size() ; i++) {
        histogram[PackedInts.bitsRequired(maxValues.get(i))]++;
      }
      return histogram;
    }

    public int getMaxBPV() {
      return maxBPV;
    }

    /**
     * @param tailBPV the bpv for the tail of the structure. Valid values: 1-63.
     * @return true if it is possible to construct a DualPlaneMutable with the given tailBPV.
     */
    public boolean isViable(int tailBPV) {
      return estimatedMem[tailBPV] != 0;
    }

    /**
     * @param tailBPV the bpv for the tail of the structure. Valid values: 1-63.
     * @return the approximate number of bytes that will be used for a DualPlaneMutable with given tailBPV.
     */
    public long getMemEstimate(int tailBPV) {
      return estimatedMem[tailBPV];
    }

    /**
     * @param tailBPV the bpv for the tail of the structure. Valid values: 1-63.
     * @return DualPlaneMutable(tailBPV) mem / Packed64 mem.
     */
    public double getFractionEstimate(int tailBPV) {
      return getMemEstimate(tailBPV) / (valueCount * maxBPV / 8.0);
    }

    /**
     * @return the tailBPV that will result in the smallest DualPlaneMutable structure. 0 if no viable tailBPV exists.
     */
    public int getMostCompactTailBPV() {
      long bestMem = Long.MAX_VALUE;
      int bestTailBPV = 0;
      for (int tailBPV = 1 ; tailBPV < 64 ; tailBPV++) {
        if (estimatedMem[tailBPV] == 0) {
          continue;
        }
        if (bestMem > estimatedMem[tailBPV]) {
          bestMem = estimatedMem[tailBPV];
          bestTailBPV = tailBPV;
        }
      }
      return bestTailBPV;
    }
  }

  // Faster than {@code set(get(index)+1)}
 // No guard against overflow
  @Override
  public STATUS incrementStatus(int index) {
    final long tailVal = tail.get(index);
    final long newVal = tailVal+1;
    if ((tailVal & headBit) == 0) { // Only defined in tail
      if (newVal < headBit) { // Fits in tail
        tail.set(index, newVal);
      } else { // Must put it in head
        head.set(headPos, newVal);
        tail.set(index, headBit | headPos++);
      }
      return tailVal == 0 ? STATUS.wasZero : STATUS.ok;
    }
    // Already defined in head
    final int headIndex = (int) (tailVal & tailValueMask);
    final long realNewVal = head.get(headIndex)+1;
    head.set(headIndex, realNewVal);
    return STATUS.ok;
  }

  @Override
  public void increment(int index) {
    incrementStatus(index);
  }

  @Override
  public boolean compareAndSet(int index, long expect, long update) {
    throw new UnsupportedOperationException("compareAndSet cannot be guaranteed for dual plane");
  }

  @Override
  public boolean hasCompareAndSet() {
    return false;
  }

  @Override
  public long get(int docID) {
    final long tailVal = tail.get(docID);
    return (tailVal & headBit) == 0 ?
        tailVal :
        head.get((int) (tailVal & tailValueMask));
  }

  @Override
  public void set(int index, long value) {
    final long tailVal = tail.get(index);
    if ((tailVal & headBit) == 0) { // Only defined in tail
      if (value < headBit) { // Fits in tail
        tail.set(index, value);
      } else { // Must put it in head
        head.set(headPos, value);
        tail.set(index, headBit | headPos++);
      }
    } else { // Already defined in head
      head.set((int) (tailVal & tailValueMask), value);
    }
  }

  @Override
  public void clear() {
    head.clear();
    headPos = 0;
    tail.clear();
  }

  @Override
  public int getBitsPerValue() {
    return head.getBitsPerValue(); // Max
  }

  public int getHeadBPV() {
    return head.getBitsPerValue();
  }
  public int getTailBPV() {
    return tail.getBitsPerValue();
  }

  @Override
  public int size() {
    return tail.size();
  }

  @Override
  public long ramBytesUsed() {
    return head.ramBytesUsed() + tail.ramBytesUsed();
  }

  public String toString() {
    return "DualPlaneMutable(head(" + head.size() + " values, " + head.getBitsPerValue() + " bpv), "
        + "tail(" + tail.size() + " values, " + tail.getBitsPerValue() + " bpv (incl. 1 pointer))";
  }

}

package org.apache.lucene.util.packed;

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


import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;

/**
 * Highly experimental structure for holding counters with known maxima.
 * The structure works best with long tail distributed maxima. The order
 * of the maxima are assumed to be random. For sorted maxima, a structure
 * of a little less than half the size is possible.
 * </p><p>
 * Space saving is prioritized very high, while performance is prioritized
 * very low. Practical usage of the structure is thus limited.
 * </p><p>
 * Warning: This representation does not support persistence yet.
 */
public class LongTailBitPlaneMutable extends PackedInts.Mutable {
  private static final int DEFAULT_OVERFLOW_BUCKET_SIZE = 1000; // Not performance tested

  private final Plane[] planes;

  public LongTailBitPlaneMutable(PackedInts.Reader maxima) {
    this(maxima, DEFAULT_OVERFLOW_BUCKET_SIZE);
  }

  public LongTailBitPlaneMutable(PackedInts.Reader maxima, int overflowBucketSize) {
    final long[] histogram = getHistogram(maxima);
    int maxBit = getMaxBit(histogram);

    List<Plane> lPlanes = new ArrayList<>(64);
    int bit = 1; // All values require at least 0 bits
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      for (int extraBit = 1; extraBit < maxBit - bit; extraBit++) {
        if (histogram[bit + extraBit] * 2 < histogram[bit]) {
          break;
        }
        extraBitsCount++;
      }
//      System.out.println(String.format("Plane bit %d + %d with size %d", bit, extraBitsCount, histogram[bit]));

      final int planeMaxBit = bit + extraBitsCount;
      lPlanes.add(new Plane((int) histogram[bit], 1 + extraBitsCount,
          planeMaxBit < maxBit, overflowBucketSize, planeMaxBit));
      bit += 1 + extraBitsCount;
    }
    planes = lPlanes.toArray(new Plane[lPlanes.size()]);
    populateStaticStructures(maxima);
  }

  // pos 0 = first bit
  public static long estimateBytesNeeded(long[] histogram) {
    System.arraycopy(histogram, 0, histogram, 1, histogram.length-1);
    histogram[0] = 0;
    for (int i = 1 ; i < histogram.length ; i++) {
      for (int j = i-1 ; j >= 0 ; j--) {
        histogram[j] += histogram[i];
      }
    }
    int maxBit = getMaxBit(histogram);

    long mem = 0;
    int bit = 1; // All values require at least 0 bits
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      for (int extraBit = 1; extraBit < maxBit - bit; extraBit++) {
        if (histogram[bit + extraBit] * 2 < histogram[bit]) {
          break;
        }
        extraBitsCount++;
      }
      final int planeMaxBit = bit + extraBitsCount;
      // Yes, very ugly. We should calculate this without temporarily constructing the plane
      mem += new Plane((int) histogram[bit], 1 + extraBitsCount,
          planeMaxBit < maxBit, DEFAULT_OVERFLOW_BUCKET_SIZE, planeMaxBit).ramBytesUsed();
      bit += 1 + extraBitsCount;
    }
    return mem;
  }

  private int max(long[] histogram, int startBit) {
    long max = histogram[startBit];
    for (int i = startBit+1 ; i < histogram.length ; i++) {
      if (histogram[i] > max) {
        max = histogram[i];
      }
    }
    return (int) max;
  }

  private void populateStaticStructures(PackedInts.Reader maxima) {
//    System.out.println("Populating " + planes.length + " planes with overflow data. Initial empty layout:");
//    for (Plane plane: planes) {
//      System.out.println(plane.toString());
//    }

    final int[] overflowIndex = new int[planes.length];
    int bit = 1;
    for (int planeIndex = 0; planeIndex < planes.length-1; planeIndex++) { // -1: Never set overflow bit on topmost
      final Plane plane = planes[planeIndex];
      for (int i = 0; i < maxima.size(); i++) {
        if (bit == 1 || (maxima.get(i) >>> planes[planeIndex - 1].maxBit) != 0) {
          final long maxValue = maxima.get(i);
          final int cacheIndex = overflowIndex[planeIndex]/plane.overflowBucketSize;
          if (cacheIndex > 0 && overflowIndex[planeIndex] % plane.overflowBucketSize == 0) { // Over the edge
            plane.overflowCache.set(cacheIndex, plane.overflowCache.get(cacheIndex - 1)); // Transfer previous sum
          }

          if (maxValue >>> plane.maxBit != 0) {
            plane.overflows.fastSet(overflowIndex[planeIndex]);
            plane.overflowCache.set(cacheIndex, plane.overflowCache.get(cacheIndex) + 1);
          }
          overflowIndex[planeIndex]++;
        }
      }
      bit += plane.values.getBitsPerValue();
    }
//    System.out.println("Finished populating " + planes.length + " planes with overflow data. Overflow flagged layout:");
//    for (Plane plane: planes) {
//      System.out.println(plane.toString());
//    }
  }

  public static int getMaxBit(long[] histogram) {
    int maxBit = 0;
    for (int bit = 0 ; bit < histogram.length ; bit++) {
      if (histogram[bit] != 0) {
        maxBit = bit;
      }
    }
    return maxBit;
  }

  // histogram[0] = total count
  private long[] getHistogram(PackedInts.Reader maxima) {
    final long[] histogram = new long[65];
    for (int i = 0 ; i < maxima.size() ; i++) {
      int bitsRequired = PackedInts.bitsRequired(maxima.get(i));
      for (int br = 1; br <=bitsRequired ; br++) {
        histogram[br]++;
      }
    }
    histogram[0] = maxima.size();
//    System.out.println("histogram: " + toString(histogram));
    return histogram;
  }

  private String toString(long[] histogram) {
    StringBuilder sb = new StringBuilder();
    for (long valueCount : histogram) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(valueCount));
    }
    return sb.toString();
  }

  @Override
  public long get(int index) {
    long value = 0;
    int shift = 0;
    // Move up in the planes until there are no more overflow-bits
    for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
      final Plane plane = planes[planeIndex];
      value |= plane.values.get(index) << shift;
      if (planeIndex == planes.length-1 || !plane.overflows.get(index)) { // Finished
        break;
      }
      shift += plane.values.getBitsPerValue();
      index = plane.getNextPlaneIndex(index);
    }
    return value;
  }

  @Override
  public void set(int index, long value) {
//    System.out.println("\nset(" + index + ", " + value + ")");
    for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
      final Plane plane = planes[planeIndex];
      final int bpv = plane.values.getBitsPerValue();

      plane.values.set(index, value & ~(~1 << (bpv-1)));
      if (planeIndex == planes.length-1 || !plane.overflows.get(index)) {
        break;
      }
      // Overflow-bit is set. We need to go up a level, even if the value is 0, to ensure full reset of the bits
      value = value >> bpv;
      index = plane.getNextPlaneIndex(index);
    }
//    for (Plane plane: planes) {
//      System.out.println(plane.toString());
//    }
  }

  // Inc should really be part of the PackedInts.Mutable API
  public void inc(int index) {
//    System.out.println("\ninc(" + index+ ")");
    for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
      final Plane plane = planes[planeIndex];
      final int bpv = plane.values.getBitsPerValue();
      long value = plane.values.get(index);
      value++;
      plane.values.set(index, value & ~(~1 << (bpv-1)));
      if (planeIndex == planes.length-1 || (value >>> bpv) == 0) { // No overflow
        break;
      }
      // We know there is actual overflow. As this is an inc, we know the overflow is 1
      index = plane.getNextPlaneIndex(index);
    }
//    for (Plane plane: planes) {
//      System.out.println(plane.toString());
//    }
  }

  @Override
  public int size() {
    return planes.length == 0 ? 0 : planes[0].values.size();
  }

  @Override
  public int getBitsPerValue() {
    return planes.length == 0 ? 0 : planes[planes.length-1].maxBit;
  }

  @Override
  public void clear() {
    for (Plane plane: planes) {
      plane.values.clear();
    }
  }

  @Override
  public long ramBytesUsed() {
    long bytes = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
    for (Plane plane: planes) {
      bytes += plane.ramBytesUsed();
    }
    return bytes;
  }

  public String toString(boolean verbose) {
    StringBuilder sb = new StringBuilder();
    if (verbose) {
      for (Plane plane: planes) {
        sb.append(plane.toString());
        sb.append("\n");
      }
    } else {
      sb.append(super.toString());
    }

    return sb.toString();
  }

  /**
   * The bits needed for a counter are divided among 1 or more planes.
   * Each plane holds n bits, where n > 0.
   * Besides the bits themselves, each plane (except the last) holds 1 bit for each value,
   * signalling if the value overflows onto the next plane.
   * </p><p>
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
  private static class Plane {
    private final PackedInts.Mutable values;
    private final OpenBitSet overflows;
    private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
    private final int overflowBucketSize;
    private final int maxBit; // Max up to this point

    public Plane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
      System.out.println(String.format("Creating plane(#values=%d, bpv=%d, overflow=%b, maxBit=%d)",
          valueCount, bpv, hasOverflow, maxBit));
      values = PackedInts.getMutable(valueCount, bpv, PackedInts.COMPACT);
      overflows = new OpenBitSet(hasOverflow ? valueCount : 0);
      this.overflowBucketSize = overflowBucketSize;
      overflowCache = PackedInts.getMutable( // TODO: Spare the +1
          valueCount / overflowBucketSize + 1, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
      this.maxBit = maxBit;
    }

    public long ramBytesUsed() {
      return RamUsageEstimator.alignObjectSize(
          3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) +
          values.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + overflows.size() / 8 +
          overflowCache.ramBytesUsed();
    }

    // Using the overflow and overflowCache, calculate the index into the next plane
    public int getNextPlaneIndex(int index) {
      int startIndex = 0;
      int nextIndex = 0;
      if (index >= overflowBucketSize) {
        nextIndex = (int) overflowCache.get(index / overflowBucketSize -1);
        startIndex = index / overflowBucketSize * overflowBucketSize;
      }
      // It would be nice to use cardinality in this situation, but that only works on the full bitset(?)
      for (int i = startIndex; i <= index; i++) {
        if (overflows.fastGet(i)) {
          nextIndex++;
        }
      }
      return nextIndex-1;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < values.getBitsPerValue(); i++) {
        sb.append(String.format("Values(%2d): ", maxBit - values.getBitsPerValue() + i));
        toString(sb, values, i);
        sb.append("\n");
      }
      sb.append("Overflow:   ");
      toString(sb, overflows);
      sb.append(" cache: ");
      toString(sb, overflowCache);
      return sb.toString();
    }

    private final int MAX_PRINT = 20;

    private void toString(StringBuilder sb, PackedInts.Mutable values, int bit) {
      for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
        sb.append((values.get(i) >> bit) & 1);
      }
    }

    private void toString(StringBuilder sb, PackedInts.Mutable values) {
      for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
        sb.append(values.get(i)).append(" ");
      }
    }

    private void toString(StringBuilder sb, OpenBitSet overflow) {
      for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
        sb.append(overflow.get(i) ? "*" : "-");
      }
    }
  }
}

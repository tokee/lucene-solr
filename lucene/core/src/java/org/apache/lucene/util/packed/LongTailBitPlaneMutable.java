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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Highly experimental structure for holding counters with known maxima.
 * The structure works best with long tail distributes maxima, although
 * the ordering of the maxima is assumed to be random. For sorted maxima,
 * a structure of a little less than half the size is possible.
 * </p><p>
 * Space saving is prioritized very high, while performance is prioritized
 * very low. Practical usage of the structure is thus limited.
 * </p><p>
 * Warning: This representation does not support persistence yet.
 */
public class LongTailBitPlaneMutable extends PackedInts.Mutable {
  private static final int DEFAULT_OVERFLOW_BUCKET_SIZE = 1000; // Not performance tested

  private final PackedInts.Mutable[] planes;
  private final int cacheChunkSize;
  private final int[][] overflowCache; // [planeID][count(cacheChunkSize)]
  private final int maxBit;

  public LongTailBitPlaneMutable(PackedInts.Reader maxima) {
    this(maxima, DEFAULT_OVERFLOW_BUCKET_SIZE);
  }

  public LongTailBitPlaneMutable(PackedInts.Reader maxima, int overflowBucketSize) {
    this.cacheChunkSize = overflowBucketSize;
    final long[] histogram = getHistogram(maxima);
    maxBit = getMaxBit(histogram);

    List<PackedInts.Mutable> lPlanes = new ArrayList<>(64);
    int bit = 0;
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      for (int extraBit = 1 ; extraBit < maxBit-bit ; extraBit++) {
        if (histogram[bit+extraBit]*2 < histogram[bit]) {
          break;
        }
        extraBitsCount++;
      }
//      System.out.println(String.format("Plane bit %d + %d with size %d", bit, extraBitsCount, histogram[bit]));
      if (bit == maxBit) { // Last plane so no overflow
        lPlanes.add(new Packed64((int) histogram[bit], 1+extraBitsCount));
      } else { // 1 plane for valueBit, 1 for overflow
        lPlanes.add(new Packed64((int) histogram[bit], 2+extraBitsCount));
      }
      bit += 1 + extraBitsCount;
    }
    planes = lPlanes.toArray(new PackedInts.Mutable[lPlanes.size()]);
    overflowCache = new int[planes.length][];
    for (int planeIndex = 0 ; planeIndex < planes.length ; planeIndex++) {
      overflowCache[planeIndex] = new int[planes[planeIndex].size() / overflowBucketSize + 1];
    }
    populateStaticStructures(maxima);
  }

  private int getMaxBit(long[] histogram) {
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
      histogram[PackedInts.bitsRequired(maxima.get(i))]++;
    }
    histogram[0] = maxima.size();
    return histogram;
  }

  @Override
  public long get(int index) {
    long value = 0;
    int shift = 0;
    // Move up in the planes until there are no more overflow-bits
    for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
      PackedInts.Mutable plane = planes[planeIndex];
      final long planeVal = plane.get(index);
      final int bpv = plane.getBitsPerValue();
      long counterVal = bpv == 1 ?
          planeVal : // last plane
          planeVal >> 1; // Remove the overflow bit
      value |= counterVal << shift;
      if (planeIndex == planes.length-1 || (planeVal & 1) != 1) { // Finished
        break;
      }

      // Raising level increases counter bits in powers of 2
      shift += bpv - 1;
      index = getNextPlaneIndex(planeIndex, index);
    }
    return value;
  }


  private void populateStaticStructures(PackedInts.Reader maxima) {
    // TODO: Implement this
  }

  @Override
  public void set(int index, long value) {
    for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
      PackedInts.Mutable plane = planes[planeIndex];
      final int bpv = plane.getBitsPerValue();
      if (planeIndex == planes.length-1) { // Last plane so just store the bits
        plane.set(index, value & ~(~1 << bpv));
        break;
      }
      // We know there's an overflow bit
      long setVal =  (value & ~(~1 <<(bpv-1))) << 1;
      value = value >> bpv-1;
      if (value != 0 || (plane.get(index) & 1) != 0) { // Once overflow is set, it must stay
        setVal |= 1;
      }
      plane.set(index, setVal);
      index = getNextPlaneIndex(planeIndex, index);
    }
  }

  // Find new index by counting previous overflow-bits
  private int getNextPlaneIndex(int planeIndex, int valueIndex) {
    int newIndex = 0;
    int overflowPos = 0;
    // Use the cache Luke
    int validCaches = (valueIndex - 1) / cacheChunkSize;
    int[] bucket = overflowCache[planeIndex];
    for (int bucketIndex = 0; bucketIndex < validCaches; bucketIndex++) {
      overflowPos += cacheChunkSize;
      newIndex += bucket[bucketIndex];
    }
    // Fine-count the rest
    PackedInts.Mutable plane = planes[planeIndex];
    while (overflowPos < valueIndex) {
      newIndex += plane.get(overflowPos++) & 1;
    }
    return newIndex;
  }

  @Override
  public int size() {
    return planes.length == 0 ? 0 : planes[0].size();
  }

  @Override
  public int getBitsPerValue() {
    return maxBit;
  }

  @Override
  public void clear() {
    for (PackedInts.Mutable plane: planes) {
      plane.clear();
    }
    for (int[] overflowBucket: overflowCache) {
      Arrays.fill(overflowBucket, 0);
    }
  }

  @Override
  public long ramBytesUsed() {
    long bytes = 0;
    for (PackedInts.Mutable plane: planes) {
      bytes += plane.ramBytesUsed();
    }
    for (int[] overflowBucket: overflowCache) {
      bytes += overflowBucket.length*4;
    }
    // TODO: Use RamUsageEstimator to include object overhead etc.
    return bytes;
  }
}

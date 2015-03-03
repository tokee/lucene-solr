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
 * a structure of roughly half the size is possible.
 * </p><p>
 * Space saving is prioritized very high, while performance is prioritized
 * very low. Practical usage of the structure is limited.
 * </p><p>
 * If the structure worked on the full list of known maxima instead of the
 * histogram over maxima, it would be possible to chunk it for better
 * compression.
 * </p><p>
 * Warning: This representation does not support persistence yet.
 */
public class LongTailBitPlaneMutable extends PackedInts.Mutable {
  private static final int DEFAULT_OVERFLOW_BUCKET_SIZE = 1000; // Not performance tested

  private final PackedInts.Mutable[] planes;
  private final int overflowBucketSize;
  private final int[][] overflowBuckets; // cache
  private final int maxBit;

  public LongTailBitPlaneMutable(long[] histogram) {
    this(histogram, DEFAULT_OVERFLOW_BUCKET_SIZE);
  }
  // histogram[0] = total counter amount
  public LongTailBitPlaneMutable(long[] histogram, int overflowBucketSize) {
    if (histogram.length != 64) {
      throw new IllegalArgumentException("The histogram length must be exactly 64, but it was " + histogram.length);
    }
    this.overflowBucketSize = overflowBucketSize;

    int maxBit = 0;
    for (int bit = 0 ; bit < histogram.length ; bit++) {
      if (histogram[bit] != 0) {
        maxBit = bit;
      }
    }
    this.maxBit = maxBit;

    List<PackedInts.Mutable> lPlanes = new ArrayList<>(64);
    int bit = 0;
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      long extraCount = 0;
      for (int extraBit = bit+1 ; extraBit <= maxBit ; extraBit++) {
        extraCount += histogram[extraBit];
        if (extraCount*2 >= histogram[bit]) {
          break;
        }
        extraBitsCount++;
      }
      if (bit == maxBit) { // Last plane so no overflow
        lPlanes.add(new Packed64((int) histogram[bit], 1+extraBitsCount));
      } else { // 1 plane for valueBit, 1 for overflow
        lPlanes.add(new Packed64((int) histogram[bit], 2+extraBitsCount));
      }
      bit += 1 + extraBitsCount;
    }
    planes = lPlanes.toArray(new PackedInts.Mutable[lPlanes.size()]);

    overflowBuckets = new int[planes.length][];
    for (int planeIndex = 0 ; planeIndex < planes.length ; planeIndex++) {
      overflowBuckets[planeIndex] = new int[planes[planeIndex].size() / overflowBucketSize + 1];
    }
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
    int validCaches = (valueIndex - 1) / overflowBucketSize;
    int[] bucket = overflowBuckets[planeIndex];
    for (int bucketIndex = 0; bucketIndex < validCaches; bucketIndex++) {
      overflowPos += overflowBucketSize;
      newIndex += bucket[bucketIndex];
    }
    // Fine-count the rest
    PackedInts.Mutable plane = planes[planeIndex];
    while (overflowPos < valueIndex) {
      newIndex += plane.get(overflowPos) & 1;
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
    for (int[] overflowBucket: overflowBuckets) {
      Arrays.fill(overflowBucket, 0);
    }
  }

  @Override
  public long ramBytesUsed() {
    long bytes = 0;
    for (PackedInts.Mutable plane: planes) {
      bytes += plane.ramBytesUsed();
    }
    for (int[] overflowBucket: overflowBuckets) {
      bytes += overflowBucket.length*4;
    }
    // TODO: Use RamUsageEstimator to include object overhead etc.
    return bytes;
  }
}

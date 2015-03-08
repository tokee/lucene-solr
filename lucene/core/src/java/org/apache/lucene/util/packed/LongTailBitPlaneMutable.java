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


import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.OpenBitSet;

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

  private final Plane[] planes;

  private class Plane {
    private final PackedInts.Mutable values;
    private final OpenBitSet overflows;
    private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
    private final int overflowBucketSize;
    private final int maxBit; // Max up to this point

    public Plane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
      values = PackedInts.getMutable(valueCount, bpv, PackedInts.COMPACT);
      overflows = new OpenBitSet(hasOverflow ? valueCount : 0);
      this.overflowBucketSize = overflowBucketSize;
      overflowCache = PackedInts.getMutable(
          valueCount/overflowBucketSize, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
      this.maxBit = maxBit;
    }
  }

  public LongTailBitPlaneMutable(PackedInts.Reader maxima) {
    this(maxima, DEFAULT_OVERFLOW_BUCKET_SIZE);
  }

  public LongTailBitPlaneMutable(PackedInts.Reader maxima, int overflowBucketSize) {
    final long[] histogram = getHistogram(maxima);
    int maxBit = getMaxBit(histogram);

    List<Plane> lPlanes = new ArrayList<>(64);
    int bit = 0;
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      for (int extraBit = 1; extraBit < maxBit - bit; extraBit++) {
        if (histogram[bit + extraBit] * 2 < histogram[bit]) {
          break;
        }
        extraBitsCount++;
      }
//      System.out.println(String.format("Plane bit %d + %d with size %d", bit, extraBitsCount, histogram[bit]));
      bit += 1 + extraBitsCount;

      lPlanes.add(new Plane((int) histogram[bit], 1 + extraBitsCount, bit < maxBit, overflowBucketSize, bit));
    }
    planes = lPlanes.toArray(new Plane[lPlanes.size()]);
    populateStaticStructures(maxima, histogram);
  }


  private void populateStaticStructures(PackedInts.Reader maxima, long[] histogram) {
    final int[] overflowIndex = new int[planes.length];
    int bit = 0;
    for (int planeIndex = 0; planeIndex < planes.length-1; planeIndex++) { // -1: Never set overflow bit on topmost
      final Plane plane = planes[planeIndex];
      for (int i = 0; i < maxima.size(); i++) {
        if (bit == 0 || planes[planeIndex - 1].overflows.fastGet(i)) {
          final long maxValue = maxima.get(i);
          if (maxValue >>> plane.maxBit != 0) {
            plane.overflows.fastSet(overflowIndex[planeIndex]);
          }
          overflowIndex[planeIndex]++;

          // Update cache
          final int cacheIndex = overflowIndex[planeIndex]/plane.overflowBucketSize;
          if (overflowIndex[planeIndex] % plane.overflowBucketSize == 0) {
            plane.overflowCache.set(cacheIndex, plane.overflowCache.get(cacheIndex-1)+1);
          } else {
            plane.overflowCache.set(cacheIndex, plane.overflowCache.get(cacheIndex));
          }
        }
      }
      bit += plane.values.getBitsPerValue();
    }
  }


  public LongTailBitPlaneMutable(long[] shift, int overflowBucketSize) {
    this(new Packed64(1, 2), 3); // TODO: Implement this
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
      Plane plane = planes[planeIndex];
      final long counterVal = plane.values.get(index);
      final int bpv = plane.values.getBitsPerValue();
      value |= counterVal << shift;
      if (planeIndex == planes.length-1 || !plane.overflows.get(index)) { // Finished
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

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

import java.util.ArrayList;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.BPVProvider;

/**
 * Setup values for plane NPlaneMutable construction.
 */
public class NPlaneLayout extends ArrayList<Plane.PseudoPlane> {
  /**
   * Generate layout intended for later creation of a NPlaneMutable.
   * Layouts does a fair amount of pre-processing and can be used for instantiating multiple NPlaneMutables.
   * @return a layout for later instantiation of NPlaneMutables.
   */
  public static NPlaneLayout getLayout(
      BPVProvider maxima, int overflowBucketSize, int maxPlanes, double collapseFraction, boolean zeroTracked) {
    return getLayoutWithZeroHistogram(
        getZeroBitHistogram(maxima, zeroTracked), overflowBucketSize, maxPlanes, collapseFraction, zeroTracked);
  }

  public static NPlaneLayout getLayout(long[] histogram, boolean zeroTracked) {
    return getLayout(histogram, NPlaneMutable.DEFAULT_MAX_PLANES, zeroTracked);
  }

  public static NPlaneLayout getLayout(long[] histogram, int maxPlanes, boolean zeroTracked) {
    return getLayoutWithZeroHistogram(
        NPlaneMutable.directHistogramToFullZero(histogram),
        NPlaneMutable.DEFAULT_OVERFLOW_BUCKET_SIZE, maxPlanes,
        NPlaneMutable.DEFAULT_COLLAPSE_FRACTION, zeroTracked);
  }

  static NPlaneLayout getLayoutWithZeroHistogram(
      long[] zeroHistogram, int overflowBucketSize, int maxPlanes, double collapseFraction, boolean zeroTracked) {
    int maxBit = NPlaneMutable.getMaxBit(zeroHistogram);

    NPlaneLayout layout = new NPlaneLayout();
    int bit = 1; // All values require at least 1 bit
    while (bit <= maxBit) { // What if maxBit == 64?
      int extraBitsCount = 0;
      if (bit == 1 && zeroTracked) {
        extraBitsCount = 0; // First plane must be 1 bit for zeroTracked to work
      } else if (((double)zeroHistogram[bit]/zeroHistogram[0] <= collapseFraction) || layout.size() == maxPlanes-1) {
        extraBitsCount = maxBit-bit;
      } else {
        for (int extraBit = 1; extraBit < maxBit - bit; extraBit++) {
          if (zeroHistogram[bit + extraBit] * 2 < zeroHistogram[bit]) {
            break;
          }
          extraBitsCount++;
        }
      }
//      System.out.println(String.format("Plane bit %d + %d with size %d", bit, extraBitsCount, histogram[bit]));

      final int planeMaxBit = bit + extraBitsCount;
      layout.add(new Plane.PseudoPlane((int) zeroHistogram[bit], 1 + extraBitsCount,
          planeMaxBit < maxBit, overflowBucketSize, planeMaxBit));
      bit += 1 + extraBitsCount;
    }
    return layout;
  }

  // histogram[0] = total count
  // Special histogram where the counts are summed downwards
  // TODO: Replace this with BPVProvider.StatCollectingBPVWrapper
  private static long[] getZeroBitHistogram(BPVProvider maxima, boolean zeroTracked) {
    final long[] histogram = new long[65];
    long totalEntries = 0;
    while (maxima.hasNext()) {
      int bitsRequired;
      if (zeroTracked) {
        long ordCount = maxima.nextValue();
        bitsRequired = ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1;
      } else {
        bitsRequired = maxima.nextBPV();
      }

      for (int br = 1; br <= bitsRequired ; br++) {
        histogram[br]++;
      }
      totalEntries++;
    }
    histogram[0] = totalEntries;
//    System.out.println("histogram: " + toString(histogram));
    return histogram;
  }
}

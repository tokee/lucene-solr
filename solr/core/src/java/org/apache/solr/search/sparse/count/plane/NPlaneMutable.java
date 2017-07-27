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

package org.apache.solr.search.sparse.count.plane;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.BPVProvider;
import org.apache.solr.search.sparse.count.Incrementable;
import org.apache.solr.search.sparse.count.PackedOpportunistic;

import java.util.Locale;

/**
 * Counter structure with very high focus on memory usage. The bits representing the values are stored across
 * multiple planes, with the element count of each plane being only the number of buckets that has value maxima
 * that reaches to that plane. This means that memory usage is linear to the number of bits needed to represent
 * all possible values. See the package javadoc for details.
 *
 * The NPlaneMutable is designed for high-cardinality (millions to hundreds of millions) facets, where the
 * maxima follows a long-tail distribution (Power Law / Zipf). The order of the maxima is assumed to be random,
 * so sample maximas could be [5, 1, 12000, 2, 1, 1, 1000, 2, 100, 5000000].
 *
 *
 * Space saving is prioritized very high, with performance being secondary.
 * Practical usage of the structure is thus limited.
 * </p><p>
 * Warning: This representation does not support persistence yet.
 */
// FIXME: TestNPlaneMutable.testShiftWrongValues() shows that shift is faulty
// TODO: Create a single-phase constructor for SplitRankPlane that does not need the histogram
public class NPlaneMutable extends PackedInts.Mutable implements Incrementable {
  public static final int DEFAULT_OVERFLOW_BUCKET_SIZE = 100;
  public static final int DEFAULT_MAX_PLANES = 64; // No default limit
  public static final double DEFAULT_COLLAPSE_FRACTION = 0.01; // If there's <= 1% counters left, pack them in 1 plane

  /**
   * The different Plane implementations, each providing different trade-offs.<br/>
   * split:  Simple implementation with separate value- and overflow-bits, using a slow rank function. Deprecated.<br/>
   * shift: Cojoined value- and overflow-bits, slow rank function. Deprecated.<br/>
   * spank:  Separate value- and overflow-bits, using an efficient rand-function. Not thread-safe.<br/>
   * tank:   Same as spank but thread-safe.<br/>
   * zethra:  Same as spank/tank, but with special plane-0 that provides fast isZero bucket checks.
   */
  public enum IMPL {
    split,
    zethra,
    spank,
    tank,
    shift
  }

  /**
   * Visualizing the counters as vertical pillars of bits (marked with #):
   * <pre>
   * 4:   #
   * 3:   ## #
   * 2: # ## #
   * 1: ######
   *    ABCDEF
   * </pre>
   * The counter for term A needs 2 bits, B needs 1 bit, C needs 4, D 3, E 1 and F3.
   * </p><p>
   * Each plane represents a horizontal slice of bits, with an extra bit-set marking (using !) which values overflows
   * upto the nextBPV plane. In this sample, the planes are
   * <pre>
   * 4: #
   *    B
   *
   *    !!!!
   * 3:  ###
   * 2: ####
   *    ACDF
   *
   *    !!!!!!
   * 1: ######
   *    ABCDEF
   * </pre>
   * Note that there is no overflow for the upper-most plane as we know it is the last plane.
   * Also note that the second plane holds 2 bits (bit 2 and 3) as the need for the overflow bits makes it cheaper to
   * share those 2 bits, rather than using an extra overflow bit set.
   */
  final Plane[] planes;
  /**
   * If true, plane0 is single-bit and signals if the value at the given position has been updated.
   * The value will always be 1 after the first increment, which also means that the number of bits needed for a
   * given maxima is 1+log2(max-1). For the maxima [0, 1, 2, 3, 4, 5] that is [1, 1, 2, 3, 3, 4].
   * The idea behind zero-tracking is to provide a cheap verification of whether a value has been updated at all.
   * Without zero-tracking, log2(max) bit-planes would have to be checked for value existence. The early termination
   * works well for sparsely filled counters.
   */
  final boolean isZeroTracked;

  /**
   * Create a mutable of length {@code maxima.size()} capable of holding values up to the given maxima.
   * Space/performance trade-offs uses default values. The counter values will be initialized to 0.
   * This operation involves 2 iterations of the given maxima. If possible, use {@link #createSibling()} on an already
   * existing NPlaneMutable with the same maxima (effectively the same field on the same index version).
   * @param maxima maxima for all values.
   * @param implementation either spank (not thread safe increments) or zethra (zero-trached thread-safe) are recommended.
   */
  public NPlaneMutable(PackedInts.Reader maxima, IMPL implementation) {
    this(new BPVProvider.BPVPackedWrapper(maxima, false), DEFAULT_OVERFLOW_BUCKET_SIZE,
        DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, implementation);
  }

  /**
   * Create a mutable of length {@code maxima.size()} capable of holding values up to the given maxima.
   * Space/performance trade-offs uses default values. The counter values will be initialized to 0.
   * This operation involves 2 iterations of the given maxima. If possible, use {@link #createSibling()} on an already
   * existing NPlaneMutable with the same maxima (effectively the same field on the same index version).
   * @param maxima maxima for all values.
   * @param implementation either spank (not thread safe increments) or tank (thread safe increments) are recommended.
   * @param overflowBucketSize only used by the deprecated implementations split {@link SplitPlane} and
   *                           shift {@link ShiftPlane}.
   * @deprecated due to the indirect deprecation of overflowBucketSize.
   */
  public NPlaneMutable(PackedInts.Reader maxima, IMPL implementation, int overflowBucketSize) {
    this(new BPVProvider.BPVPackedWrapper(maxima, false), overflowBucketSize,
        DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, implementation);
  }

  private NPlaneMutable(Plane[] planes) {
    this.planes = planes;
    isZeroTracked = planes.length > 0 && planes[0] instanceof SplitRankZeroPlane &&
        ((SplitRankZeroPlane)planes[0]).zeroTracker != null;
  }

  /**
   * Create a mutable of length {@code maxima.size()} capable of holding values up to the given maxima.
   * The counter values will be initialized to 0.
   * This operation involves 2 iterations of the given maxima. If possible, use {@link #createSibling()} on an already
   * existing NPlaneMutable with the same maxima (effectively the same field on the same index version).
   * @param maxima maxima for all values.
   * @param overflowBucketSize only used by the deprecated implementations split {@link SplitPlane} and
   *                           shift {@link ShiftPlane}.
   * @param maxPlanes the maximum number of planes to create.
   * @param collapseFraction if the number of remaining buckets at a given plane gets below this fraction of the
   *                         total bucket number, the BPV for that plane will be set so that it is the last plane.
   * @param implementation either spank (not thread safe increments) or tank (thread safe increments) are recommended.
   * @deprecated due to the indirect deprecation of overflowBucketSize.
   */
  public NPlaneMutable(
      BPVProvider maxima, int overflowBucketSize, int maxPlanes, double collapseFraction, IMPL implementation) {
    this(NPlaneLayout.getLayout(maxima, overflowBucketSize, maxPlanes, collapseFraction, implementation == IMPL.zethra),
        maxima, implementation);
  }

  /**
   * Create a mutable of length {@code maxima.size()} capable of holding values up to the given maxima.
   * Space/performance trade-offs uses default values. The counter values will be initialized to 0.
   * This operation involves 1 iteration of the given maxima. If possible, use {@link #createSibling()} on an already
   * existing NPlaneMutable with the same maxima (effectively the same field on the same index version).
   * @param layout holds plane-layout information from a previous iteration of maxima. Normally not called directly.
   * @param maxima maxima for all values.
   * @param implementation either spank (not thread safe increments) or tank (thread safe increments) are recommended.
   * @deprecated due to the indirect deprecation of overflowBucketSize.
   */
  public NPlaneMutable(NPlaneLayout layout, BPVProvider maxima, IMPL implementation) {
    planes = new Plane[layout.size()];
    for (int i = 0 ; i < layout.size() ; i++) {
      planes[i] = layout.get(i).createPlane(implementation);
    }
    isZeroTracked = planes.length > 0 && planes[0] instanceof SplitRankZeroPlane &&
        ((SplitRankZeroPlane)planes[0]).zeroTracker != null;
    populateStaticStructures(maxima);
  }

  /**
   * Fast and memory-efficient creations of NPlane sharing the same layout and properties as this.
   * This is the recommended way of creating extra NPlanes when the underlying index has not changed since the
   * creation of the initial NPlane.
   * @return a NPlane with independent counters but shared overflow layout structure.
   */
  public NPlaneMutable createSibling() {
    Plane[] newPlanes = new Plane [planes.length];
    for (int i = 0 ; i < planes.length ; i++) {
      newPlanes[i] = planes[i].createSibling();
    }
    return new NPlaneMutable(newPlanes);
  }

  /**
   * Returns a long representing 64 values, with each bit being a flag for one value.
   * If the flag for a value is 1, the corresponding value is not zero.
   * @param longIndex value-index / 64.
   * @return 64 flags for non-zero.
   */
  public long getNonZeroBits(int longIndex) {
    return planes[0].getNonZeroBits(longIndex);
  }

  // pos 0 = first bit

  /**
   * Provides an estimation of the memory needed to hold a NPlaneMutable with the given implementation.
   * @return the expected size in bytes as a sum of bucket-bits and control structures.
   */
  public static long estimateBytesNeeded(long[] histogram, IMPL implementation) {
    return estimateBytesNeeded(
        histogram, DEFAULT_OVERFLOW_BUCKET_SIZE, DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, false,
        implementation);
  }
  public static long estimateBytesNeeded(long[] histogram, IMPL implementation, boolean extraInstance) {
    return estimateBytesNeeded(
        histogram, DEFAULT_OVERFLOW_BUCKET_SIZE, DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, extraInstance,
        implementation);
  }
  public static long estimateBytesNeeded(
      long[] histogram, int overflowBucketSize, int maxPlanes, double collapseFraction, boolean extraInstance,
      IMPL impl) {
    long[] full = directHistogramToFullZero(histogram);
    long mem = 0;
    for (Plane.PseudoPlane pp: NPlaneLayout.getLayoutWithZeroHistogram(
        full, overflowBucketSize, maxPlanes, collapseFraction, impl == IMPL.zethra)) {
      mem += pp.estimateBytesNeeded(extraInstance, impl);
    }
    return mem;
  }

  static long[] directHistogramToFullZero(long[] histogram) {
//    long[] full = new long[histogram.length+1];
    long[] full = new long[histogram.length];
//    System.arraycopy(histogram, 0, full, 1, histogram.length);
    System.arraycopy(histogram, 0, full, 0, histogram.length);
    full[0] = 0;
    for (int i = 1 ; i < full.length ; i++) {
      for (int j = i-1 ; j >= 0 ; j--) {
        full[j] += full[i];
      }
    }
    return full;
  }

  /**
   * Iterates all given maxima and sets the overflow-flags for the planes making up the NPlaneMutable.
   */
  private void populateStaticStructures(BPVProvider sourceMaxima) {
    final int[] overflowIndex = new int[planes.length]; // The offsets of overflow-bits in the different planes
    final BPVProvider.BPVZeroWrapper maxima = new BPVProvider.BPVZeroWrapper(sourceMaxima);

    // Which loop is in which depends on how fast maxima iteration is. Here we assume slow maxima
    maxima.reset();
    while (maxima.hasNext()) {
      final int bpv = isZeroTracked ? maxima.nextZeroBPV() : maxima.nextBPV();
      int bit = 1;
      for (int planeIndex = 0; planeIndex < planes.length-1; planeIndex++) { // -1: Never set overflow bit on topmost
        final Plane plane = planes[planeIndex];
        if (bit == 1 || (bpv - planes[planeIndex - 1].maxBit) > 0) {
          if (bpv - plane.maxBit > 0) {
            plane.setOverflow(overflowIndex[planeIndex]);
          }
          overflowIndex[planeIndex]++;
        }
        bit += plane.bpv;
      }
    }

    for (Plane plane: planes) {
      plane.finalizeOverflow();
    }
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

  @Override
  public long get(int index) {
    if (!isZeroTracked) {
      long value = 0;
      int shift = 0;
      // Move up in the planes until there are no more overflow-bits
      for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
        final Plane plane = planes[planeIndex];
        value |= plane.get(index) << shift;
        if (planeIndex == planes.length - 1 || !plane.isOverflow(index)) { // Finished
          break;
        }
        shift += plane.bpv;
        index = plane.overflowRank(index);
      }
      return value;
    }
    long value = 0;
    int shift = 0;
    final long zeroValue = planes[0].get(index);
    if (zeroValue == 0 || !planes[0].isOverflow(index)) { // Quick termination?
      return zeroValue;
    }
    // Move up in the planes until there are no more overflow-bits, starting with shift 1 due to zeroCounter
    index = planes[0].overflowRank(index);
    for (int planeIndex = 1; planeIndex < planes.length; planeIndex++) {
      final Plane plane = planes[planeIndex];
      value |= plane.get(index) << shift;
      if (planeIndex == planes.length - 1 || !plane.isOverflow(index)) { // Finished
        break;
      }
      shift += plane.bpv;
      index = plane.overflowRank(index);
    }
    return zeroValue + value;
  }

  public int getPlaneCount() {
    return planes.length;
  }

  @Override
  public void set(int index, long value) {
//    System.out.println("\nset(" + index + ", " + value + ")");
    if (!isZeroTracked) {
      for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
        final Plane plane = planes[planeIndex];

        plane.set(index, value & ~(~1L << (plane.bpv - 1)));
        if (planeIndex == planes.length - 1 || !plane.isOverflow(index)) {
          break;
        }
        // Overflow-bit is set. We need to go up a level, even if the value is 0, to ensure full reset of the bits
        value = value >> plane.bpv;
        index = plane.overflowRank(index);
      }
      return;
    }

    planes[0].set(index, value == 0 ? 0 : 1);
    value -= value == 0 ? 0 : 1;
    index = planes[0].overflowRank(index);
    for (int planeIndex = 1; planeIndex < planes.length; planeIndex++) {
      final Plane plane = planes[planeIndex];

      plane.set(index, value & ~(~1L << (plane.bpv - 1)));
      if (planeIndex == planes.length - 1 || !plane.isOverflow(index)) {
        break;
      }
      // Overflow-bit is set. We need to go up a level, even if the value is 0, to ensure full reset of the bits
      value = value >> plane.bpv;
      index = plane.overflowRank(index);
    }
//    for (Plane plane: planes) {
//      System.out.println(plane.toString());
//    }
  }

  @Override
  public void increment(final int index) {
//    System.out.println("\ninc(" + index+ ")");
    int vIndex = index;
    for (int p = 0; p < planes.length; p++) {
      Plane plane = planes[p];
      try {
        if (plane.inc(vIndex) != STATUS.overflowed) {
          // No overflow; exit immediately. Note: There is no check for overflow beyond maxima
          break;
        }
        vIndex = plane.overflowRank(vIndex);
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new ArrayIndexOutOfBoundsException(String.format(Locale.ENGLISH,
            "inc(%d) on plane %d from root inc(%d): %s",
            vIndex, p, index, plane));
      } catch (NullPointerException e) {
        NullPointerException nex = new NullPointerException(
            "Exception incrementing plane " + (p+1) + "/" + planes.length);
        nex.initCause(e);
        throw nex;
      }
      // We know there is actual overflow. As this is an inc, we know the overflow is 1
    }
  }

  /**
   * Increments the value and returns it.
   * Warning: This is a synchronized method and not recommended for high performance. Use {@link #increment(int)}
   * instead if possible.
   * @param index the index for the value to increment.
   * @return the incremented value.
   */
  @Override
  public STATUS incrementStatus(int index) {
    if (!isZeroTracked) { // Have to do it the slow way
      long original = get(index);
      increment(index);
      return original == 0 ? STATUS.wasZero : STATUS.ok;
    }

    int vIndex = index;
    for (int p = 0; p < planes.length; p++) {
      Plane plane = planes[p];
      try {
        final STATUS status = plane.inc(vIndex);
        if (p == 0 && status == STATUS.wasZero) { // First increment for this counter
          return STATUS.wasZero;
        }
        if (status != STATUS.overflowed) {
          // No overflow; exit immediately. Note: There is no check for overflow beyond maxima
          return STATUS.ok;
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new ArrayIndexOutOfBoundsException(String.format(Locale.ENGLISH,
            "inc(%d) on plane %d from root inc(%d): %s",
            vIndex, p, index, plane));
      }
      // We know there is actual overflow. As this is an inc, we know the overflow is 1
      vIndex = plane.overflowRank(vIndex);
    }
    return STATUS.ok;
  }

  @Override
  public boolean compareAndSet(int index, long expect, long update) {
    throw new UnsupportedOperationException(
        "Cannot guarantee atomicity of compareAndSet due to the multi-plane nature of NPlaneMutable");
  }

  @Override
  public boolean hasCompareAndSet() {
    return false;
  }

  @Override
  public int size() {
    return planes.length == 0 ? 0 : planes[0].valueCount;
  }

  @Override
  public int getBitsPerValue() {
    return planes.length == 0 ? 0 : planes[planes.length-1].maxBit;
  }

  @Override
  public void clear() {
    for (Plane plane: planes) {
      plane.clear();
    }
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed(false);
  }
  public long ramBytesUsed(boolean extraInstance) {
    long bytes = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
    for (Plane plane: planes) {
      bytes += plane.ramBytesUsed(extraInstance);
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

  public static PackedInts.Mutable newFromTemplate(PackedInts.Mutable original) {
    // TODO: Add dualplane
    if (original instanceof PackedOpportunistic) {
      return PackedOpportunistic.create(original.size(), original.getBitsPerValue());
    }
    if (original instanceof NPlaneMutable) {
      return ((NPlaneMutable)original).createSibling();
    }
    // Packed64, Packed64SingleBlock, Packed64SingleBlock, Packed8ThreeBlocks, Packed16ThreeBlocks,
    // Direct8, Direct16, Direct32
    // FIXME: getFormat is package private
    //return PackedInts.getMutable(original.size(), original.getBitsPerValue(), original.getFormat());
    return PackedInts.getMutable(original.size(), original.getBitsPerValue(), PackedInts.COMPACT);
  }

}

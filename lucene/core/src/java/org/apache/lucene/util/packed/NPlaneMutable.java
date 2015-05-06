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


import org.apache.lucene.util.Incrementable;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RankBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

/**
 * Highly experimental structure for holding counters with known maxima.
 * The structure works best with long tail distributed maxima.
 * The order of the maxima are assumed to be random.
 * For sorted maxima, a structure of a little less than half the size is possible,
 * but that has not been implemented.
 * </p><p>
 * Space saving is prioritized very high, with performance being secondary.
 * Practical usage of the structure is thus limited.
 * </p><p>
 * Warning: This representation does not support persistence yet.
 */
// TODO: Align caches to 64 bits and use Long.bitCount with IMPL.split
// TODO: Deprecate split as spank is always faster & smaller
// TODO: Implement shift with rank (shank)
// FIXME: TestNPlaneMutable.testShiftWrongValues() shows that shift is faulty
// TODO: Create a single-phase constructor that does not need the histogram
public class NPlaneMutable extends PackedInts.Mutable implements Incrementable {
  public static final int DEFAULT_OVERFLOW_BUCKET_SIZE = 100; // Should probably be a low lower (100 or so)
  public static final int DEFAULT_MAX_PLANES = 64; // No default limit
  public static final double DEFAULT_COLLAPSE_FRACTION = 0.01; // If there's <= 1% counters left, pack them in 1 plane

  public static enum IMPL {split, spank, tank, zethra, shift}

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
   * </p>
   */
  final Plane[] planes;
  final boolean isZeroTracked;

  /**
   * Create a mutable of length {@code maxima.size()} capable of holding values up to the given maxima.
   * Space/performance trade-offs uses default values. The values will be initialized to 0.
   * @param maxima maxima for all values.
   * @param implementation either spank (not thread safe increments) or tank (thread safe increments) are recommended.
   */
  public NPlaneMutable(PackedInts.Reader maxima, IMPL implementation) {
    this(new BPVPackedWrapper(maxima, false), DEFAULT_OVERFLOW_BUCKET_SIZE,
        DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, implementation);
  }

  public NPlaneMutable(PackedInts.Reader maxima, IMPL implementation, int overflowBucketSize) {
    this(new BPVPackedWrapper(maxima, false), overflowBucketSize,
        DEFAULT_MAX_PLANES, DEFAULT_COLLAPSE_FRACTION, implementation);
  }

  private NPlaneMutable(Plane[] planes) {
    this.planes = planes;
    isZeroTracked = planes.length > 0 && planes[0] instanceof SplitRankZeroPlane &&
        ((SplitRankZeroPlane)planes[0]).zeroTracker != null;
  }

  public NPlaneMutable(
      BPVProvider maxima, int overflowBucketSize, int maxPlanes, double collapseFraction, IMPL implementation) {
    this(getLayout(maxima, overflowBucketSize, maxPlanes, collapseFraction, implementation == IMPL.zethra),
        maxima, implementation);
  }
  public NPlaneMutable(Layout layout, BPVProvider maxima, IMPL implementation) {
    planes = new Plane[layout.size()];
    for (int i = 0 ; i < layout.size() ; i++) {
      planes[i] = layout.get(i).createPlane(implementation);
    }
    isZeroTracked = planes.length > 0 && planes[0] instanceof SplitRankZeroPlane &&
        ((SplitRankZeroPlane)planes[0]).zeroTracker != null;
    populateStaticStructures(maxima);
  }

  /**
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

  /**
   * Generate layout intended for later creation of a finn NPlaneMutable.
   * Layouts does a fair amount of pre-processing and can be used for
   * instantiating multiple NPlaneMutables.
   * @return a layout for later instantiation of NPlaneMutables.
   */
  public static Layout getLayout(
      BPVProvider maxima, int overflowBucketSize, int maxPlanes, double collapseFraction, boolean zeroTracked) {
    return getLayoutWithZeroHistogram(
        getZeroBitHistogram(maxima, zeroTracked), overflowBucketSize, maxPlanes, collapseFraction, zeroTracked);
  }
  public static Layout getLayout(long[] histogram, boolean zeroTracked) {
    return getLayout(histogram, DEFAULT_MAX_PLANES, zeroTracked);
  }
  public static Layout getLayout(long[] histogram, int maxPlanes, boolean zeroTracked) {
    return getLayoutWithZeroHistogram(directHistogramToFullZero(histogram), DEFAULT_OVERFLOW_BUCKET_SIZE, maxPlanes,
        DEFAULT_COLLAPSE_FRACTION, zeroTracked);
  }
  static Layout getLayoutWithZeroHistogram(
      long[] zeroHistogram, int overflowBucketSize, int maxPlanes, double collapseFraction, boolean zeroTracked) {
    int maxBit = getMaxBit(zeroHistogram);

    Layout layout = new Layout();
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
      layout.add(new PseudoPlane((int) zeroHistogram[bit], 1 + extraBitsCount,
          planeMaxBit < maxBit, overflowBucketSize, planeMaxBit));
      bit += 1 + extraBitsCount;
    }
    return layout;
  }

  /**
   * Pre-calculated setup values for plane NPlaneMutable construction.
   */
  public static class Layout extends ArrayList<PseudoPlane> { }

  // pos 0 = first bit
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
    for (PseudoPlane pp: getLayoutWithZeroHistogram(
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

  private void populateStaticStructures(BPVProvider sourceMaxima) {
    final int[] overflowIndex = new int[planes.length];
    final BPVZeroWrapper maxima = new BPVZeroWrapper(sourceMaxima);

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

  // histogram[0] = total count
  // Special histogram where the counts are summed downwards
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

  /**
   * A description of a plane. Used for estimations.
   */
  static class PseudoPlane {
    private final int valueCount;
    private final int bpv;
    private final boolean hasOverflows;
    private final int maxBit;
    private final int overflowBucketSize;

    public String toString() {
      return String.format("PseudoPlane(#values=%9d, bpv=%2d, overflows=%5b, maxBits=%2d)",
          valueCount, bpv, hasOverflows, maxBit);
    }

    private PseudoPlane(int valueCount, int bpv, boolean hasOverflows, int overflowBucketSize, int maxBit) {
      this.valueCount = valueCount;
      this.bpv = bpv;
      this.hasOverflows = hasOverflows;
      this.maxBit = maxBit;
      this.overflowBucketSize = overflowBucketSize;
    }

    public Plane createPlane(IMPL impl) {
      switch (impl) {
        case split:  return new SplitPlane(valueCount, bpv, hasOverflows, overflowBucketSize, maxBit);
        case spank:  return new SplitRankPlane(valueCount, bpv, hasOverflows, maxBit, false);
        case tank:   return new SplitRankPlane(valueCount, bpv, hasOverflows, maxBit, true);
        case zethra: return new SplitRankZeroPlane(valueCount, bpv, hasOverflows, maxBit, true);
        case shift:  return new ShiftPlane(valueCount, bpv, hasOverflows, overflowBucketSize, maxBit);
        default: throw new UnsupportedOperationException("No Plane implementation available for " + impl);
      }
    }

    public long estimateBytesNeeded(boolean extraInstance, IMPL impl) {
      switch (impl) {
        case split: {
          long bytes = RamUsageEstimator.alignObjectSize(
              3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) + // Plane object
              RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 1L*valueCount*bpv/8; // Values, assuming compact
          if (!extraInstance) {
            bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER; // overflow header
            if (hasOverflows) {
              bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 1L*valueCount/8 + // overflow bits
                  valueCount/overflowBucketSize*PackedInts.bitsRequired(valueCount)/8; // Cache
            }
          }
          return bytes;
        }
        case spank:
        case zethra:
        case tank: {
          long bytes = RamUsageEstimator.alignObjectSize(
              3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) + // Plane object
              RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 1L*valueCount*bpv/8; // Values, assuming compact
          if (!extraInstance) {
            bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER; // overflow header
            if (hasOverflows) {
              bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + valueCount/8 + // overflow bits
                  ((long)valueCount)*64/2048; // Rank cache uses a long for every 2048 bits
            }
          }
          return bytes;
        }
        case shift: return -1;
        // TODO: Add shift
        default: throw new UnsupportedOperationException("No memory estimation available for " + impl);
      }
    }
  }

  /**
   * The bits needed for a counter are divided among 1 or more planes.
   * Each plane holds n bits, where n > 0.
   * Besides the bits themselves, each plane (except the last) holds 1 bit for each value,
   * signalling if the value overflows onto the nextBPV plane.
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
  public abstract static class Plane {
    public final int valueCount;
    public final int bpv;
    public final int maxBit; // Max up to this point
    public final boolean hasOverflow;

    public Plane(int valueCount, int bpv, int maxBit, boolean hasOverflow) {
      this.valueCount = valueCount;
      this.bpv = bpv;
      this.maxBit = maxBit;
      this.hasOverflow = hasOverflow;
    }
    /**
     * Increment the value at the given index by 1.
     * </p><p>
     * @param index the value to increment.
     * @return semi-detailed info about the operation.
     */
    public abstract STATUS inc(int index);

    // max of 2^bpv-1 must be ensured by the caller
    public abstract void set(int index, long value);

    public abstract long ramBytesUsed(boolean extraInstance);

    /**
     * @param index relative to the conceptual overflow bits.
     * @return the number of set overflow bits up to but not including the given index.
     */
    public abstract int overflowRank(int index);

    public abstract void setOverflow(int index);
    public abstract boolean isOverflow(int index);

    public abstract void finalizeOverflow();

    public long ramBytesUsed() {
      return ramBytesUsed(false);
    }

    public abstract void clear();

    public abstract long get(int index);

    public String toString() {
      return "Plane(#values=" + valueCount + ", bpv=" + bpv + ", maxBit=" + maxBit + ", overflow=" + hasOverflow + ")";
    }

    /**
     * Creates a new Plane that shares overflow structures with the current Plane.
     * Used to get multiple counters with the same maxima.
     * The counter bits for the constructed Plane will be empty.
     * @return a Plane sharing components with this Plane, but with independent counter values.
     */
    public abstract Plane createSibling();

    /**
     * Returns a long representing 64 values, with each bit being a flag for one value.
     * If the flag for a value is 1, the corresponding value is not zero.
     * @param longIndex value-index / 64.
     * @return 64 flags for non-zero.
     */
    public abstract long getNonZeroBits(int longIndex);
  }

  private static class SplitPlane extends Plane {
    private final PackedInts.Mutable values;     // TODO: Make an incrementable version
    private final OpenBitSet overflows;
    private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
    private final int overflowBucketSize;

    public SplitPlane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
      super(valueCount, bpv, maxBit, hasOverflow);
      values = PackedInts.getMutable(valueCount, bpv, PackedInts.COMPACT);
      overflows = new OpenBitSet(hasOverflow ? valueCount : 0);
      this.overflowBucketSize = overflowBucketSize;
      overflowCache = PackedInts.getMutable( // TODO: Spare the +1
          valueCount / overflowBucketSize + 1, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
    }

    private SplitPlane(int valueCount, int bpv, int maxBit, boolean hasOverflow, PackedInts.Mutable values,
                       OpenBitSet overflows, PackedInts.Mutable overflowCache, int overflowBucketSize) {
      super(valueCount, bpv, maxBit, hasOverflow);
      this.values = values;
      this.overflows = overflows;
      this.overflowCache = overflowCache;
      this.overflowBucketSize = overflowBucketSize;
    }

    @Override
    public Plane createSibling() {
      return new SplitPlane(
          valueCount, bpv, maxBit, hasOverflow, newFromTemplate(values), overflows, overflowCache, overflowBucketSize);
    }

    @Override
    public STATUS inc(int index) {
      long value = values.get(index);
      value++;
      values.set(index, value & ~(~1 << (values.getBitsPerValue()-1)));
      return value == 1 ? STATUS.wasZero : (value >>> values.getBitsPerValue()) != 0 ? STATUS.overflowed : STATUS.ok;
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
      overflows.fastSet(index);
    }

    @Override
    public void finalizeOverflow() {
      if (!hasOverflow) {
        return;
      }
      for (int i = 0; i < valueCount; i++) {
        final int cacheIndex = i/overflowBucketSize;
        if (cacheIndex > 0 && i%overflowBucketSize == 0) { // Over the edge
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
      long bytes =RamUsageEstimator.alignObjectSize(
          3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) + values.ramBytesUsed();
      if (!extraInstance) {
        bytes += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + overflows.size() / 8 + overflowCache.ramBytesUsed();
      }
      return bytes;
    }

    // Using the overflow and overflowCache, calculate the index into the nextBPV plane
    @Override
    public int overflowRank(int index) {
      int startIndex = 0;
      int rank = 0;
      if (index >= overflowBucketSize) {
        rank = (int) overflowCache.get(index / overflowBucketSize -1);
        startIndex = index / overflowBucketSize * overflowBucketSize;
      }
      // It would be nice to use cardinality in this situation, but that only works on the full bitset(?)
      for (int i = startIndex; i < index; i++) {
        if (overflows.fastGet(i)) {
          rank++;
        }
      }
      return rank;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < values.getBitsPerValue(); i++) {
        sb.append(String.format(Locale.ENGLISH, "Values(%2d): ", maxBit - values.getBitsPerValue() + i));
        NPlaneMutable.toString(sb, values, i);
        sb.append("\n");
      }
      sb.append("Overflow:   ");
      NPlaneMutable.toString(sb, overflows);
      sb.append(" cache: ");
      NPlaneMutable.toString(sb, overflowCache);
      return sb.toString();
    }

    @Override
    public long getNonZeroBits(int longIndex) {
      throw new UnsupportedOperationException("Non-zero bits are not fast to calculate for this implementation");
    }
  }

  /**
   * Separate storage of value bits and overflow bits, where overflow bits are shared across instances.
   */
  private static class SplitRankPlane extends Plane {
    protected final PackedInts.Mutable values;
    protected final Incrementable incValues;
    protected final RankBitSet overflows;

    public SplitRankPlane(int valueCount, int bpv, boolean hasOverflow, int maxBit, boolean threadGuarded) {
      super(valueCount, bpv, maxBit, hasOverflow);
      values = getPacked(valueCount, bpv, threadGuarded);
      incValues = values instanceof Incrementable ? (Incrementable)values : new IncrementableMutable(values);
      overflows = new RankBitSet(hasOverflow ? valueCount : 0);
    }

    private SplitRankPlane(PackedInts.Mutable values, RankBitSet overflows, int maxBit, boolean hasOverflow, int bpv) {
      super(values.size(), bpv, maxBit, hasOverflow);
      this.values = values;
      incValues = values instanceof Incrementable ? (Incrementable)values : new IncrementableMutable(values);
      this.overflows = overflows;
    }

    @Override
    public Plane createSibling() {
      return new SplitRankPlane(values, overflows, maxBit, hasOverflow, bpv);
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
    public STATUS inc(int index) {
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
      overflows.fastSet(index);
    }

    @Override
    public void finalizeOverflow() {
      if (hasOverflow) {
        overflows.buildRankCache();
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
      long bytes =RamUsageEstimator.alignObjectSize(
          3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) + values.ramBytesUsed();
      if (!extraInstance) {
        bytes += overflows.ramBytesUsed();
      }
      return bytes;
    }

    @Override
    public int overflowRank(int index) {
      if (overflows != null) {
        try {
          return overflows.rank(index);
        } catch (NullPointerException e) {
          throw new NullPointerException("Exceeded overflow bits for plane " + this.getClass().getSimpleName() + " with"
              + " maxBit=" + maxBit + " and #overflow-bits=" + overflows.size() + " when requesting index " + index);
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
        NPlaneMutable.toString(sb, values, i);
        sb.append("\n");
      }
      sb.append("Overflow:   ");
      NPlaneMutable.toString(sb, overflows);
      return sb.toString();
    }
  }

  private static class SplitRankZeroPlane extends SplitRankPlane {
    protected final PackedOpportunistic.PackedOpportunistic1 zeroTracker; // Only defined for first plane

    public SplitRankZeroPlane(int valueCount, int bpv, boolean hasOverflow, int maxBit, boolean threadGuarded) {
      super(valueCount, bpv, hasOverflow, maxBit, threadGuarded);
      zeroTracker = values instanceof PackedOpportunistic.PackedOpportunistic1 ?
          (PackedOpportunistic.PackedOpportunistic1) values : null;
    }

    private SplitRankZeroPlane(
        PackedInts.Mutable values, RankBitSet overflows, int maxBit, boolean hasOverflow, int bpv) {
      super(values, overflows, maxBit, hasOverflow, bpv);
      zeroTracker = values instanceof PackedOpportunistic.PackedOpportunistic1 ?
          (PackedOpportunistic.PackedOpportunistic1) values : null;
    }
    @Override
    public STATUS inc(int index) {
      return zeroTracker == null ? incValues.incrementStatus(index) : zeroTracker.incrementCeilStatus(index);
    }

    @Override
    public Plane createSibling() {
      return new SplitRankZeroPlane(values, overflows, maxBit, hasOverflow, bpv);
    }

    @Override
    protected PackedInts.Mutable getPacked(int valueCount, int bpv, boolean threadGuarded) {
      // Always use the thread safe as that is the only one that has a zeroTracked inner counter
      for (int candidateBPV = bpv; candidateBPV < 64 ; candidateBPV++) {
        if (PackedOpportunistic.isSupported(candidateBPV)) {
          return PackedOpportunistic.create(valueCount, candidateBPV);
        }
      }
      throw new IllegalArgumentException("Unable to create PackedOpportunistic with BitsPerValue="  + bpv);
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

  private static class ShiftPlane extends Plane {
    private final PackedInts.Mutable values;
    private final PackedInts.Mutable overflowCache; // [count(cacheChunkSize)]
    private final int overflowBucketSize;
    private final long VALUE_BITS_MASK;

    public ShiftPlane(int valueCount, int bpv, boolean hasOverflow, int overflowBucketSize, int maxBit) {
      super(valueCount, bpv, maxBit, hasOverflow);
      values = PackedInts.getMutable(valueCount, hasOverflow ? bpv+1 : bpv, PackedInts.COMPACT);
      this.overflowBucketSize = overflowBucketSize;
      overflowCache = PackedInts.getMutable( // TODO: Spare the +1
          valueCount / overflowBucketSize + 1, PackedInts.bitsRequired(valueCount), PackedInts.COMPACT);
      VALUE_BITS_MASK = ~(~1L << (bpv-1));
    }

    private ShiftPlane(PackedInts.Mutable values, PackedInts.Mutable overflowCache, boolean hasOverflow,
                      int overflowBucketSize, int maxBit, int bpv) {
      super(values.size(), bpv, maxBit, hasOverflow);
      this.values = values;
      this.overflowCache = overflowCache;
      this.overflowBucketSize = overflowBucketSize;
      VALUE_BITS_MASK = ~(~1L << (bpv-1));
    }

    // In reality this shares only the overflowCache with siblings, not the overflow bits
    @Override
    public Plane createSibling() {
      PackedInts.Mutable emptyValues = newFromTemplate(values);
      if (hasOverflow) { // Copy the overflow bits
        for (int i = 0 ; i < valueCount ; i++) {
          emptyValues.set(i, values.get(i) & 0x1);
        }
      }
      return new ShiftPlane(emptyValues, overflowCache, false, overflowBucketSize, maxBit, bpv);
    }

    @Override
    public STATUS inc(int index) {
      long value;
      if (hasOverflow) {
        final long rawValue = values.get(index);
        value = rawValue >>> 1;
        value++;
        values.set(index, ((value & VALUE_BITS_MASK) << 1) | (rawValue & 0x1));
      } else {
        value = values.get(index) + 1;
        values.set(index, value & VALUE_BITS_MASK);
      }
      return value == 1 ? STATUS.wasZero : (value >>> bpv) != 0 ? STATUS.overflowed : STATUS.ok;
    }

    @Override
    public void set(int index, long value) {
      if (hasOverflow) {
        values.set(index, (value << 1) | (values.get(index) & 0x1));
      } else {
        values.set(index, value);
      }
    }

    @Override
    public boolean isOverflow(int index) {
      return (values.get(index) & 0x1) == 1;
    }

    // Nukes existing values
    @Override
    public void setOverflow(int index) {
      values.set(index, 1);
    }

    @Override
    public void finalizeOverflow() {
      if (!hasOverflow) {
        return;
      }
      for (int i = 0; i < valueCount; i++) {
        final int cacheIndex = i/overflowBucketSize;
        if (cacheIndex > 0 && i%overflowBucketSize == 0) { // Over the edge
          overflowCache.set(cacheIndex, overflowCache.get(cacheIndex - 1)); // Transfer previous sum
        }

        if (!isOverflow(i)) {
          continue;
        }
        overflowCache.set(cacheIndex, overflowCache.get(cacheIndex) + 1);
      }
    }

    // Quite heavy as we cannot use Arrays.fill underneath
    @Override
    public void clear() {
      if (!hasOverflow) {
        values.clear();
        return;
      }
      for (int i = 0; i < valueCount ; i++) {
        values.set(i, values.get(i) & 0x1);
      }
    }

    @Override
    public long get(int index) {
      return hasOverflow ? values.get(index) >>> 1 : values.get(index);
    }

    @Override
    public long ramBytesUsed(boolean extraInstance) {
      return RamUsageEstimator.alignObjectSize(
          2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_INT) +
          values.ramBytesUsed() + overflowCache.ramBytesUsed();
    }

    // Using the overflow and overflowCache, calculate the index into the nextBPV plane
    @Override
    public int overflowRank(int index) {
      int startIndex = 0;
      int rank = 0;
      if (index >= overflowBucketSize) {
        rank = (int) overflowCache.get(index / overflowBucketSize -1);
        startIndex = index / overflowBucketSize * overflowBucketSize;
      }
      // It would be nice to use cardinality in this situation, but that only works on the full bitset(?)
      for (int i = startIndex; i < index; i++) {
        if (isOverflow(i)) {
          rank++;
        }
      }
      return rank;
    }

    @Override
    public long getNonZeroBits(int longIndex) {
      throw new UnsupportedOperationException("Non-zero bits are not fast to calculate for this implementation");
    }

    public String toString() {
      return "ShiftPlane(" + super.toString() + ")";
    }
  }

  private static int MAX_PRINT = 20;

  private static void toString(StringBuilder sb, PackedInts.Mutable values, int bit) {
    for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
      sb.append((values.get(i) >> bit) & 1);
    }
  }

  private static void toString(StringBuilder sb, PackedInts.Mutable values) {
    for (int i = 0; i < MAX_PRINT && i < values.size(); i++) {
      sb.append(values.get(i)).append(" ");
    }
  }

  private static void toString(StringBuilder sb, OpenBitSet overflow) {
    for (int i = 0; i < MAX_PRINT && i < overflow.size(); i++) {
      sb.append(overflow.get(i) ? "*" : "-");
    }
  }

  /**
   * Describes counter layout in the form of bits required to represent each counter.
   */
  public static interface BPVProvider {
    /**
     * @return true if there is more bitsRequired available.
     */
    public boolean hasNext();

    /**
     *  @return the nextBPV bitsRequired.
     */
    public int nextBPV();

    /**
     * Optional method.
     * @return the nextBPV raw value.
     */
    public long nextValue();

    /**
     * Reset the structure for another iteration.
     */
    public void reset();
  }

  public static class BPVPackedWrapper implements BPVProvider {
    private final PackedInts.Reader maxima;
    private final boolean alreadyBPV;
    private int pos = 0;

    public BPVPackedWrapper(PackedInts.Reader maxima, boolean alreadyBPV) {
      this.maxima = maxima;
      this.alreadyBPV = alreadyBPV;
    }

    public int size() {
      return maxima.size();
    }

    @Override
    public boolean hasNext() {
      return pos < maxima.size();
    }

    @Override
    public int nextBPV() {
      return alreadyBPV ? (int) maxima.get(pos++) : PackedInts.bitsRequired(maxima.get(pos++));
    }

    @Override
    public long nextValue() {
      if (alreadyBPV) {
        throw new UnsupportedOperationException("The maxima are represented as BPV internally. The raw value is lost");
      }
      return maxima.get(pos++);
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  public static class BPVIntArrayWrapper implements BPVProvider {
    private final int[] maxima;
    private final boolean alreadyBPV;
    private int pos = 0;

    public BPVIntArrayWrapper(int[] maxima, boolean alreadyBPV) {
      this.maxima = maxima;
      this.alreadyBPV = alreadyBPV;
    }

    public int size() {
      return maxima.length;
    }

    @Override
    public boolean hasNext() {
      return pos < maxima.length;
    }

    @Override
    public int nextBPV() {
      return alreadyBPV ? maxima[pos++] : PackedInts.bitsRequired(maxima[pos++]);
    }

    @Override
    public long nextValue() {
      if (alreadyBPV) {
        throw new UnsupportedOperationException("The maxima are represented as BPV internally. The raw value is lost");
      }
      return maxima[pos++];
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  public static class BPVAbsorber {
    private final PackedInts.Mutable bpvs;
    private int pos = 0;
    public BPVAbsorber(int size, int maxBPV) {
      bpvs = PackedInts.getMutable(size, maxBPV, PackedInts.COMPACT);
    }
    public void addBPV(int bpv) {
      bpvs.set(pos++, bpv);
    }
    public void addAbsolute(long maxValue) {
      bpvs.set(pos++, PackedInts.bitsRequired(maxValue));
    }
    public BPVProvider getProvider() {
      return new BPVPackedWrapper(bpvs, true);
    }
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
    return PackedInts.getMutable(original.size(), original.getBitsPerValue(), original.getFormat());
  }

  public static class BPVZeroWrapper implements BPVProvider {
    private final BPVProvider source;

    public BPVZeroWrapper(BPVProvider source) {
      this.source = source;
    }

    public int nextZeroBPV() {
      final long ordCount = (int) source.nextValue();
      return ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public int nextBPV() {
      return source.nextBPV();
    }

    @Override
    public long nextValue() {
      return source.nextValue();
    }

    @Override
    public void reset() {
      source.reset();
    }
  }

  public static class StatCollectingBPVWrapper implements BPVProvider {
    private final BPVProvider source;

    public long entries = 0;
    public long maxCount = -1;
    public long refCount = 0;
    public final long[] histogram = new long[64];
    /*
    The plusOneHistogram is used by {@link NPlaneMutable} for creating an instance optimized for cheap
    checks for counter values different from 0.
    For details, see https://sbdevel.wordpress.com/2015/04/30/alternative-counter-tracking/
    Decimal  0  1   2    3    4     5     6     7     8
    Binary   0  1  10   11  100   101   110   111  1000
    Binary+  0  1  11  101  111  1001  1011  1101  1111
     */
    public final long[] plusOneHistogram = new long[64];

    public StatCollectingBPVWrapper(BPVProvider source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public int nextBPV() {
      final long ordCount = (int) source.nextValue();
      final int bpv = PackedInts.bitsRequired(ordCount);
      { // Statistics collection
        histogram[bpv]++;
        plusOneHistogram[ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return bpv;
    }
    @Override
    public long nextValue() {
      final long ordCount = (int) source.nextValue();
      { // Statistics collection
        final int bpv = PackedInts.bitsRequired(ordCount);
        histogram[bpv]++;
        plusOneHistogram[ordCount <= 1 ? 1 : PackedInts.bitsRequired(ordCount-1)+1]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return ordCount;
    }

    @Override
    public void reset() {
      source.reset();
      entries = 0;
      maxCount = -1;
      refCount = 0;
      Arrays.fill(histogram, 0);
    }

    /**
     * Convenience method if the only purpose is to collect statistics.
     */
    public void collect() {
      while (hasNext()) {
        nextValue();
      }
    }

    @Override
    public String toString() {
      return "StatCollectingBPVWrapper(" +
          "entries=" + entries + ", maxCount=" + maxCount + ", refCount=" + refCount +
          ", histogram=" + Arrays.toString(histogram) + ", plusOneHistogram=" + Arrays.toString(plusOneHistogram) + ')';
    }
  }
}

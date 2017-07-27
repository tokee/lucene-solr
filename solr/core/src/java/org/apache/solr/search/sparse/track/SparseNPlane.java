package org.apache.solr.search.sparse.track;

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

import org.apache.solr.search.sparse.SparseKeys;
import org.apache.solr.search.sparse.count.Incrementable;
import org.apache.solr.search.sparse.count.plane.NPlaneMutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Special tracker that wraps NPlaneMutable, using two layers of bitmaps to track updated values.
 * As the trackers only need to track non-zero longs in the underlying bits, the memory overhead for the first
 * tracking layer is 1/64 (1.56%) and the overhead for the second tracking layer is 1/64² (0.02%).
 *
 * Only recommended for NPlaneMutables of implementation {@link NPlaneMutable.IMPL#zethra} as that is the only one
 * that provides fast {@link org.apache.solr.search.sparse.count.plane.Plane#getNonZeroBits(int)}.
 *
 * Semi-thread-safe (concurrent calls to {@link #inc(int)} are safe), if the provided {@link #counts} is thread safe.
 */
// TODO: Performance test whether 1 or 2 levels of trackers are optimal
// It is unclear whether the second tracker is worth it. As is, 1M buckets can be tracked at level 1 with only
// 1M/64² longs = 244 longs. Even at the 100M-level of facet cardinality, it dies not seem excessive to simply
// interate all entries in the level 1 tracker.
public class SparseNPlane implements ValueCounter {
  public static Logger log = LoggerFactory.getLogger(SparseNPlane.class);

  /**
   * The core counter structure.
   */
  private final NPlaneMutable counts;
  /**
   * If {@link #counts} is not {@link Incrementable}, it will be wrapped to adhere to that interface.
   * If it is Incrementable, countsInc will be equal to counts.
   */
  private final Incrementable countsInc;
  /**
   * Level 1 tracking of which blocks (64 bit longs) has been updated in {@link #counts}.
   */
  private final AtomicLongArray tracker;
  /**
   * Level 2 tracking of which blocks (64 bit longs) has been updated in {@link #tracker}.
   */
  private final AtomicLongArray trackerTracker; // Tracks changes to tracker
  /**
   * The number of counters that has a non-zero value. Used with {@link #tracksMax} to switch to non-tracking mode
   * if the number of updates gets too high.
   */
  private AtomicLong nonZeroCounters = new AtomicLong(0);
  /**
   * The maximum number of buckets to track. Updating more than this number of buckets disables tracking.
   */
  private final int tracksMax;
  /**
   * Whether or not any incrementation has happened at all.
   */
  // TODO: If nonZeroCounters is reliable, this should not be needed at all. Do test with explicitely disabled tracking
  private boolean incremented = false;

  /**
   * Used for generating cache-keys for the concrete SparseNPlane.
   */
  // TODO: Move the implementation-specific key generation parts to the implementations instead of doing it outside
  public final SparseKeys.COUNTER_IMPL counterImpl; // Used for key generation

  /**
   * Special counter for documents with no values.
   */
  private AtomicLong missing = new AtomicLong(0);

  /**
   * The minimum amount of unique tags in the facet for sparse tracking to activate.
   * Using tracking with low-cardinality fields is counter-productive (as is the use of SparseNPlane for those fields).
   * Sane values are around 10K-1M, depending on concrete setup.
   */
  private final int minCountsForSparse; //
  /**
   * The fraction of the amount of unique tags in the facet that should be the limit for sparse tracking.
   * If the number of unique updated buckets (there is 1 bucket/tag) exceeds this, tracking is disabled for the
   * current run.
   */
  private final double fraction;        // The fractional size of tracker (#trackers=#counts*fraction)
  /**
   * True if tracking was explicitly disabled by calling {@link #disableSparseTracking()}.
   */
  private boolean explicitlyDisabled = false;

  /**
   * Identifies this instance with respect to the request that caused it. Used with a cache for re-using
   * the filled buckets.
   */
  private String contentKey = null;

  /**
   * @param counts            a update-thread-safe counter. Must implement {@link Incrementable}.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting.
   */
  public SparseNPlane(SparseKeys.COUNTER_IMPL counterImpl, NPlaneMutable counts,
                      int minCountsForSparse, double fraction) {
    this.counterImpl = counterImpl;
    this.counts = counts;
    if (!(counts instanceof Incrementable)) {
      throw new UnsupportedOperationException("The given counter must implement Incrementable but was " + counts);
    }
    this.countsInc = (Incrementable)counts;
    this.minCountsForSparse = minCountsForSparse;
    this.fraction = fraction;

    // Disable tracking if the counter is too small for it to make sense
    if (counts.size() < minCountsForSparse) {
      tracksMax = 0;
      tracker = null;
      trackerTracker = null;
    } else {
      tracksMax = (int) (counts.size() * fraction);
      tracker = new AtomicLongArray(counts.size()/64/64+1);           // +1 to round up
      trackerTracker = new AtomicLongArray(tracker.length()/64+1);    // +1 to round up
      // FIXME: An occasional (q:"d-mol" at SB test installation) ArrayIndexOutOfBounds seems workarounded by the +64
//      trackerTracker = new AtomicLongArray(counts.size()/64/64/64 +1); // +1 to round up
//      tracker = new AtomicLongArray(counts.size()/64/64 +64+1);           // Why the +1?
    }
  }

  @Override
  public ValueCounter createSibling() {
    //newCounter.setContentKey(getContentKey()); // Values are zeroed, so no contentKey
    // TODO: Call createSibling in NPlaneMutable instead of newFromTemplate
    return new SparseNPlane(counterImpl, (NPlaneMutable) NPlaneMutable.newFromTemplate(counts),
        minCountsForSparse, fraction);
  }

  /**
   * Constructs an ID which is unique for the given layout, but not for the content of the buckets.
   * Used for lookup of cached counters in {@link org.apache.solr.search.sparse.cache.SparseCounterPool}.
   */
  public static String createStructureKey(
      int counts, int minCountForSparse, double fraction, SparseKeys.COUNTER_IMPL counter) {
    // TODO: Check if the field name is needed to ensure uniqueness
    return "SparseNPlane(counts" + counts
        + "minCountsForSparse" + minCountForSparse + "fraction" + fraction + "counter=" + counter +")";
  }

  /**
   * @return a key derived from the construction parameters. Used to group compatible SparseCounters.
   */
  @Override
  public String getStructureKey() {
    //return SparseCounter.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction);
    return SparseNPlane.createStructureKey(counts.size(), minCountsForSparse, fraction, counterImpl);
  }

  /**
   * Increments the given counter.
   * This method is thread-safe.
   * @param counter the index of the counter to increment.
   */
  @Override
  public final void inc(int counter) {
    incremented = true;  // Very congested here. This might imply a CPU cache flush delay
    if (nonZeroCounters.get() >= tracksMax) {
      // The tracker has been disabled or its capacity exceeded, so we just update the value
      countsInc.increment(counter);
      return;
    }

    // We want to track changes to counters to maintain the sparse structure
    if(countsInc.incrementStatus(counter) == Incrementable.STATUS.wasZero) {
      // This is the first update of the counter, so we add it to the tracker
      // TODO: Further optimize with blockWasZero as we only tracks blocks here
      updateTracker(counter);
    }
  }

  /**
   * Flag the block (long) containing the counter as updated in the tracker structure.
   * This method is thread-safe.
   */
  private void updateTracker(int counter) {
    nonZeroCounters.incrementAndGet(); // Hot spot that we could mitigate with Incrementable.STATUS.blockWasZero
    final int trackerIndex = counter >>> 12; //     /64/64
    final int trackerBit = (counter >>> 6) & 63; // /64%64
    while (true) {
      long oldValue = tracker.get(trackerIndex);
      long newValue = oldValue | (1L << trackerBit);
      if (newValue == oldValue) { // Already tracked
        break;
      }

      if (tracker.compareAndSet(trackerIndex, oldValue, newValue)) {
        // TODO: Skip if oldValue!=0 ?
        updateTrackerTracker(trackerIndex);
        break;
      }
    }
  }
  /**
   * Flag the block (long) containing the tracker-1 flag as updated in the tracker structure.
   * This method is thread-safe.
   */
  private void updateTrackerTracker(int trackerIndex) {
    final int ttIndex = trackerIndex >>> 6; // /64
    final int ttBit = trackerIndex & 63; // /  %64
    while (true) {
      long oldValue = trackerTracker.get(ttIndex);
      long newValue = oldValue | (1L << ttBit);
      if (newValue == oldValue || trackerTracker.compareAndSet(ttIndex, oldValue, newValue)) {
        break;
      }
    }
  }

  @Override
  public void incMissing() {
    missing.incrementAndGet();
  }

  @Override
  public long getMissing() {
    return missing.get();
  }

  // TODO: It should be possible to make this thread-safe if all Planes are thread-safe themselves
  @Override
  public boolean hasThreadSafeInc() {
    return false;
  }

  /**
   * Set the given bucket (aka counter) to the given value.
   *
   * This method is not thread-safe: The bits from the values from concurrent calling threads can be intermingled
   * due to the plane-splitting.
   */
  @Override
  public final void set(int counter, long value) {
    // getAndSet would be nice here to guard against double tracking with concurrent
    // setting of a value to the same counter
    long oldValue = counts.get(counter);
    counts.set(counter, value);
    if (oldValue == 0) {
      updateTracker(counter);
    }
  }

  /**
   * If the cut-off point for tracking has not been reached, clear time is linear to updated elements.
   * If it has been reached, clear time is linear to the total number of buckets.
   */
  @Override
  public void clear() {
    final int nonZero = (int) nonZeroCounters.getAndSet(0);
    final boolean hasBeenIncremented = incremented;
    incremented = false;
    explicitlyDisabled = false;
    missing.set(0);
    setContentKey(null);
    if (!hasBeenIncremented) {
      return;
    }

    // The tracking limit has been exceeded: Clear all values and all tracking structures.
    if (nonZero >= tracksMax || tracker == null) {
      counts.clear();
      if (tracker != null) {
        for (int i = 0 ; i < tracker.length() ; i++) { // If only we had access to the inner long[]...
          tracker.set(i, 0);
        }
        for (int i = 0 ; i < trackerTracker.length() ; i++) {
          trackerTracker.set(i, 0);
        }
      }
      return;
    }
    // 0 < nonZero < tracksMax (considered sparse)
    // Tracking limit has not ben exceeded. Clear is performed only on the buckets that has been updated.
    int cleared = 0;
    out:
    for (int tti = 0 ; tti < trackerTracker.length() ; tti++) { // For all trackertracker entries
      long ttBitset = trackerTracker.getAndSet(tti, 0);
      while (ttBitset != 0) {                 // While the trackertracker entry has bits
        final long tt = ttBitset & -ttBitset; // Get the position of the rightmost bit
        final int ti = Long.bitCount(tt-1);
        ttBitset ^= tt;                       // Clear the rightmost bit

        long tBitset = tracker.getAndSet(tti * 64 + ti, 0); // Get the tracker entry
        while (tBitset != 0) {                // While the trackertracker entry has bits
          final long t = tBitset & -tBitset;  // Get the position of the rightmost bit
          final int i = Long.bitCount(t-1);
          tBitset ^= t;                       // Clear the rightmost bit

          long vBitset = counts.getNonZeroBits(tti*64*64 + ti*64 + i); // Get the counter block for plane 0
          while (vBitset != 0) {              // While the counter block has updated buckets
            final long v = vBitset & -vBitset;// Get the position of the rightmost bit
            final int z = Long.bitCount(v-1);
            vBitset ^= v;                     // Clear the rightmost bit

            counts.set(tti*64*64*64 + ti*64*64 + i*64 + z, 0); // Clear the bucket
            if (cleared++ == nonZero) { // Terminate early if all updated buckets has been cleared
              break out;
            }
          }
        }
      }
    }
  }
/*

    int tti = 0;
    int ti, i, z;
    while (cleared != nonZero && tti < trackerTracker.length()) {
      long ttv = trackerTracker.getAndSet(tti, 0);
      while ((ti = Long.numberOfLeadingZeros(ttv)) != 64 && cleared != nonZero) {
        ttv = 1L << ti;
        long tv = tracker.getAndSet(tti * 64 + ti, 0);
        while ((i = Long.numberOfLeadingZeros(tv)) != 64 && cleared != nonZero) {
          tv = 1L << i;
          long v = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
          while ((z = Long.numberOfLeadingZeros(v)) != 64 && cleared != nonZero) {
            v = 1L << z;
            counts.set(tti*64*64*64 + ti*64*64 + i*64 + z, 0);
            cleared++;
          }
        }
      }
    }
    }
  */
  /**
   * @return the absolute size of the counters.
   */
  @Override
  public int size() {
    return counts.size();
  }

  @Override
  public String getContentKey() {
    return contentKey;
  }

  @Override
  public void setContentKey(String contentKey) {
    this.contentKey = contentKey;
  }

  /**
   * Note: For iteration purposes, it is strongly recommended to use {@link #iterate}.
   * @param counter an absolute index in counters.
   * @return the count at the index.
   */
  @Override
  public final long get(int counter) {
    return counts.get(counter);
  }

  // This code should be kept in sync with SparseCounterInt.iterate
  @Override
  public boolean iterate(
      final int start, final int end, final int minValue, boolean doNegative, final Callback callback) {
    if (start < 0 || end > size()) {
      throw new ArrayIndexOutOfBoundsException(String.format(
          "iterate(start=%d, end=%d, minValue=%d, callback) called on counter with size=%d",
          start, end, minValue, size()));
    }

    if (nonZeroCounters.get() >= tracksMax || doNegative) { // Not sparse or very big (normally the same thing)
      callback.setOrdered(true);
      for (int counter = start ; counter < end ; counter++) {
        final long value = get(counter);
        if (doNegative || value >= minValue) {
          callback.handle(counter, value);
        }
      }
      return false;
    }

    // Sparse
    // http://lemire.me/blog/archives/2013/12/23/even-faster-bitmap-decoding/
    // See the comments in the clear() method for explanation on bit fiddling
    callback.setOrdered(minValue > 0); // Might need second run if minValue == 0
    boolean filled = false;
    final int sparseMinValue = Math.max(minValue, 1);
    for (int tti = 0 ; tti < trackerTracker.length() ; tti++) {
      long ttBitset = trackerTracker.get(tti);
      while (ttBitset != 0) {
        final long tt = ttBitset & -ttBitset;
        final int ti = Long.bitCount(tt-1);
        ttBitset ^= tt;

        long tBitset = tracker.get(tti * 64 + ti);
        while (tBitset != 0) {
          final long t = tBitset & -tBitset;
          final int i = Long.bitCount(t-1);
          tBitset ^= t;
          long vBitset = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
          while (vBitset != 0) {
            final long v = vBitset & -vBitset;
            final int z = Long.bitCount(v-1);
            vBitset ^= v;

            final int counter = tti*64*64*64 + ti*64*64 + i*64 + z;
            final long value = get(counter);
            if (counter >= start && counter <= end && value >= sparseMinValue) {
              filled |= callback.handle(counter, value);
            }
          }
        }
      }
    }
//    log.info(String.format(Locale.ENGLISH, "iterate(ttc=%d, tc=%d, c=%d, filled=%b, ms=%d)",
//        ttc, tc, c, filled, (System.nanoTime()-startTime)/1000000));
    /*
        callback.setOrdered(minValue > 0); // Might need second run if minValue == 0
    boolean filled = false;
    final int sparseMinValue = Math.max(minValue, 1);
    int tti = 0;
    int ti, i, z;
    while (tti < trackerTracker.length()) {
      long ttBitset = trackerTracker.get(tti);
      while ((ti = Long.numberOfLeadingZeros(ttBitset)) != 64) {
        ttBitset = 1L << ti;
        long tBitset = tracker.get(tti * 64 + ti);
        while ((i = Long.numberOfLeadingZeros(tBitset)) != 64) {
          tBitset = 1L << i;
          long vBitset = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
          while ((z = Long.numberOfLeadingZeros(vBitset)) != 64) {
            vBitset = 1L << z;
            final int counter = tti*64*64*64 + ti*64*64 + i*64 + z;
            long value = get(counter);
            if (counter >= start && counter <= end && value >= sparseMinValue) {
              filled |= callback.handle(counter, value);
            }
          }
        }
      }
      tti++;
    }


     */
    if (minValue == 0 && !filled) { // We need a second iteration to get enough 0-count values to fill the callback
      for (int counter = start ; counter < end ; counter++) {
        final long value = get(counter);
        if (value == 0 && counter >= start && counter <= end) {
          if (callback.handle(counter, value)) {
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Disable tracking by signalling blown track array.
   */
  @Override
  public void disableSparseTracking() {
    nonZeroCounters.set(tracksMax);
    explicitlyDisabled = true;
  }

  @Override
  public boolean explicitlyDisabled() {
    return explicitlyDisabled;
  }

  @Override
  public String toString() {
    return "SparseNPlane(counter=" + counts + ", counts=" + counts.size() + ", bpv=" + counts.getBitsPerValue()
        + ", trackers=" + nonZeroCounters + "/" + tracksMax
        + ", minCountsForSparse=" + minCountsForSparse + ", fraction=" + fraction + ", explicitly disabled="
        + explicitlyDisabled + ')';
  }
}

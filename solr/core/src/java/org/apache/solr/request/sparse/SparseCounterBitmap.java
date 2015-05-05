package org.apache.solr.request.sparse;

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
import org.apache.lucene.util.packed.NPlaneMutable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Sparse counter whick uses a bitmap for tracking updated values..
 */
public class SparseCounterBitmap implements ValueCounter {
  private final NPlaneMutable counts;  // One counter/tag
  private final Incrementable countsInc; // Wrapper around or same-reference as counts
  private final AtomicLongArray tracker; // Tracks changes to counts
  // We could have infinite tracker trackers, but as it is, two layers means 1 top entry covers 64^4 = 16M values
  private final AtomicLongArray trackerTracker; // Tracks changes to tracker
  private AtomicLong nonZeroCounters = new AtomicLong(0);
  private final int tracksMax; // The maximum amount of trackers (tracker.length)
  public final SparseKeys.COUNTER_IMPL counterImpl; // Used for key generation

  private AtomicLong missing = new AtomicLong(0);

  private final long maxCountForAny;    // The maximum count that it is possible to reach. Intended to PackedInts
  private final int minCountsForSparse; // The minimum amount of unique tags in order to perform sparse tracking at all
  private final double fraction;        // The fractional size of tracker (#trackers=#counts*fraction)
  private final long maxCountTracked;   // if any count reaches this number, it is not tracked anymore. For performance.
  private boolean explicitlyDisabled = false;

  private String contentKey = null;

  /**
   * @param counts            a update-thread-safe counter. Must implement {@link org.apache.lucene.util.Incrementable}.
   * @param maxCountGlobal    the maximum amount any count can reach.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting.
   * @param maxCountExplicit  if any count reaches this number, it is not tracked anymore. -1 disables this.
   *                          if specified, it is highly recommended to set this to 2^n-1, with 7, 255 and 65535
   *                          being the fastest.
   */
  public SparseCounterBitmap(SparseKeys.COUNTER_IMPL counterImpl, NPlaneMutable counts, long maxCountGlobal,
                             int minCountsForSparse, double fraction, long maxCountExplicit) {
    this.counterImpl = counterImpl;
    this.counts = counts;
    if (!(counts instanceof Incrementable)) {
      throw new UnsupportedOperationException("The given counter must implement Incrementable but was " + counts);
    }
    this.countsInc = (Incrementable)counts;
    this.maxCountForAny = maxCountGlobal;
    this.minCountsForSparse = minCountsForSparse;
    this.fraction = fraction;
    this.maxCountTracked = maxCountExplicit;
    if (maxCountExplicit != -1 && maxCountExplicit != Integer.MAX_VALUE && !countsInc.hasCompareAndSet()) {
      throw new IllegalArgumentException("The given counter does not support compareAndGet and maxCountTracked was "
          + maxCountExplicit + ". Explicit maxCountTracked requires compareAndGet: " + counts);
    }
    if (counts.size() < minCountsForSparse) {
      tracksMax = 0;
      tracker = null;
      trackerTracker = null;
    } else {
      tracksMax = (int) (counts.size() * fraction);
      tracker = new AtomicLongArray(counts.size()/64/64+1);
      trackerTracker = new AtomicLongArray(counts.size()/64/64/64+1); //
    }
  }

  @Override
  public ValueCounter createSibling() {
    SparseCounterBitmap newCounter = new SparseCounterBitmap(
        counterImpl, (NPlaneMutable) NPlaneMutable.newFromTemplate(counts), maxCountForAny, minCountsForSparse,
        fraction, maxCountTracked);
    newCounter.setContentKey(getContentKey());
    return newCounter;
  }

  /*
     * Constructs an ID which is unique for the given layout. Used for lookup of cached counters in
     * {@link SparseCounterPool}.
     */
  public static String createStructureKey(
      int counts, long maxCountForAny, int minCountForSparse, double fraction, long maxCountTracked,
      SparseKeys.COUNTER_IMPL counter) {
    return "SparseCounterBitmap(counts" + counts + "maxCountForAny" + maxCountForAny
        + "minCountsForSparse" + minCountForSparse + "fraction" + fraction + "maxCountTracked" + maxCountTracked
        + "counter=" + counter +")";
  }

  /**
   * @return a key derived from the construction parameters. Used to group compatible SparseCounters.
   */
  @Override
  public String getStructureKey() {
    //return SparseCounter.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction);
    return SparseCounterBitmap.createStructureKey(counts.size(), maxCountForAny,
        minCountsForSparse, fraction, maxCountTracked, counterImpl);
  }

  /**
   * Increments the given counter.
   * @param counter the index of the counter to increment.
   */
  @Override
  // This implementation has the potential of adding multiple identical entries to the
  // tracking structure for high contention multi-threading.
  // This only affects performance, not validity of the end result.
  public final void inc(int counter) {
    // No explicit max set for the for counters (this is the standard case)
    if (maxCountTracked == -1) {
      if (nonZeroCounters.get() >= tracksMax) {
       // The tracker has been exceeded or disabled, so we just update the value (very fast)
        countsInc.increment(counter);
        return;
      }
      // We want to track changes to counters to maintain the sparse structure

      if(countsInc.incrementStatus(counter) == Incrementable.STATUS.wasZero) {
        // This is the first update of the counter, so we add it to the tracker
        updateTracker(counter);
      }
      return;
    }

    // There is an explicit max (facet.sparse.maxtracked). We must avoid blowing through
    // the ceiling
    while (true) {
      final long oldValue = counts.get(counter);
      if (oldValue >= maxCountTracked) { // Max reached: Skip update
        return;
      }
      // Try to set the increased counter value opportunistically
      if (!countsInc.compareAndSet(counter, oldValue, oldValue+1)) {
        // There was a collision; another thread updated the counter before we did: Start over
        continue;
      }
      if (oldValue == 0) {
        // This was the first update of the counter: Track it
        updateTracker(counter);
      }
      break;
    }
  }

  private void updateTracker(int counter) {
    nonZeroCounters.incrementAndGet();
    final int trackerIndex = counter >>> 12; // /64/64
    final int trackerBit = (counter >>> 6) & 63; // /64%64
    while (true) {
      long oldValue = tracker.get(trackerIndex);
      long newValue = oldValue & (1 << trackerBit);
      if (newValue == oldValue) { // Already tracked
        break;
      }
      if (tracker.compareAndSet(counter, oldValue, newValue)) {
        updateTrackerTracker(trackerIndex);
        break;
      }
    }
  }
  private void updateTrackerTracker(int counter) {
    final int ttIndex = counter >>> 12; // /64/64
    final int ttBit = (counter >>> 6) & 63; // /64%64
    while (true) {
      long oldValue = trackerTracker.get(ttIndex);
      long newValue = oldValue & (1 << ttBit);
      if (newValue == oldValue || trackerTracker.compareAndSet(counter, oldValue, newValue)) {
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

  @Override
  public boolean hasThreadSafeInc() {
    return false;
  }

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
   * If the cut-off point has not been reached, clear time is linear to updated elements. If it has been reached,
   * clear time is linear to the total number of counts.
   */
  @Override
  public void clear() {
    final int nonZero = (int) nonZeroCounters.get();
    explicitlyDisabled = false;
    nonZeroCounters.set(0);
    missing.set(0);
    setContentKey(null);
    if (nonZero == 0) {
      return;
    } else if (nonZero >= tracksMax) {
      counts.clear();
      for (int i = 0 ; i < tracker.length() ; i++) { // If only we had access to the inner long[]...
        tracker.set(i, 0);
      }
      for (int i = 0 ; i < trackerTracker.length() ; i++) {
        trackerTracker.set(i, 0);
      }
      return;
    }
    // 0 < nonZero < tracksMax (considered sparse)

    int cleared = 0;
    int tti = 0;
    int ti, i, z;
    while (cleared != nonZero && tti < trackerTracker.length()) {
      long ttv = trackerTracker.getAndSet(tti, 0);
      while ((ti = Long.numberOfLeadingZeros(ttv)) != 64 && cleared != nonZero) {
        ttv = 1 << (64-ti);
        long tv = tracker.getAndSet(tti*64+ti, 0);
        while ((i = Long.numberOfLeadingZeros(tv)) != 64 && cleared != nonZero) {
          tv = 1 << (64-i);
          long v = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
          while ((z = Long.numberOfLeadingZeros(v)) != 64 && cleared != nonZero) {
            v = 1 << (64-z);
            counts.set(tti*64*64*64 + ti*64*64 + i*64 + z, 0);
            cleared++;
          }
        }
      }
    }
  }

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
    callback.setOrdered(minValue > 0); // Might need second run if minValue == 0
    boolean filled = false;
    final int sparseMinValue = Math.max(minValue, 1);
    int tti = 0;
    int ti, i, z;
    while (tti < trackerTracker.length()) {
      long ttv = trackerTracker.get(tti);
      while ((ti = Long.numberOfLeadingZeros(ttv)) != 64) {
        ttv = 1 << (64-ti);
        long tv = tracker.get(tti*64+ti);
        while ((i = Long.numberOfLeadingZeros(tv)) != 64) {
          tv = 1 << (64-i);
          long v = counts.getNonZeroBits(tti*64*64 + ti*64 + i);
          while ((z = Long.numberOfLeadingZeros(v)) != 64) {
            v = 1 << (64-z);
            final int counter = tti*64*64*64 + ti*64*64 + i*64 + z;
            long value = get(counter);
            if (counter >= start && counter <= end && value >= sparseMinValue) {
              filled |= callback.handle(counter, value);
            }
          }
        }
      }
    }

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
    return "SparseCounterBitmap(counter=" + counts + ", counts=" + counts.size() + ", bpv=" + counts.getBitsPerValue()
        + ", trackers=" + nonZeroCounters + "/" + tracksMax + ", maxCountForAny=" + maxCountForAny
        + ", minCountsForSparse=" + minCountsForSparse + ", fraction=" + fraction + ", explicitly disabled="
        + explicitlyDisabled + ')';
  }
}

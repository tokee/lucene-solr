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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.SparseKeys;
import org.apache.solr.search.sparse.count.Incrementable;
import org.apache.solr.search.sparse.count.plane.NPlaneMutable;

/**
 * Re-usable sparse counter. A mirror of {@link SparseCounterPacked} with thread-safe
 * increments and sets. For single-threaded usage, this class is slower than SparseCounterPacked.
 */
public class SparseCounterThreaded implements ValueCounter {
  final PackedInts.Mutable counts;  // One counter/tag
  private final Incrementable countsInc; // Wrapper around or same-reference as counts
  private final int[] tracker; // Tracker not PackedInts.Mutable as it should be relatively small
  private final int tracksMax; // The maximum amount of trackers (tracker.length)
  public final SparseKeys.COUNTER_IMPL counterImpl; // Used for key generation

  // The current amount of tracker entries. Setting this to 0 works as a tracker clear
  private AtomicInteger tracksPos = new AtomicInteger(0);
  private AtomicLong missing = new AtomicLong(0);

  private final long maxCountForAny;    // The maximum count that it is possible to reach. Intended to PackedInts
  private final int minCountsForSparse; // The minimum amount of unique tags in order to perform sparse tracking at all
  private final double fraction;        // The fractional size of tracker (#trackers=#counts*fraction)
  private final long maxCountTracked;   // if any count reaches this number, it is not tracked anymore. For performance.
  private boolean explicitlyDisabled = false;

  private String contentKey = null;

  /**
   * @param counts            a update-thread-safe counter. Must implement {@link Incrementable}.
   * @param maxCountGlobal    the maximum amount any count can reach.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting.
   * @param maxCountExplicit  if any count reaches this number, it is not tracked anymore. -1 disables this.
   *                          if specified, it is highly recommended to set this to 2^n-1, with 7, 255 and 65535
   *                          being the fastest.
   */
  public SparseCounterThreaded(SparseKeys.COUNTER_IMPL counterImpl, PackedInts.Mutable counts, long maxCountGlobal,
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
    } else {
      tracksMax = (int) (counts.size() * fraction);
//      tracker = PackedInts.getMutable(tracksMax, PackedInts.bitsRequired(counts), PackedInts.FAST);
      tracker = new int[tracksMax];
    }
  }

  @Override
  public ValueCounter createSibling() {
    SparseCounterThreaded newCounter = new SparseCounterThreaded(
        counterImpl, NPlaneMutable.newFromTemplate(counts), maxCountForAny, minCountsForSparse,
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
    return "SparseCounterThreaded(counts" + counts + "maxCountForAny" + maxCountForAny
        + "minCountsForSparse" + minCountForSparse + "fraction" + fraction + "maxCountTracked" + maxCountTracked
        + "counter=" + counter +")";
  }

  /**
   * @return a key derived from the construction parameters. Used to group compatible SparseCounters.
   */
  @Override
  public String getStructureKey() {
    //return SparseCounter.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction);
    return SparseCounterThreaded.createStructureKey(counts.size(), maxCountForAny,
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
      if (tracksPos.get() >= tracksMax) {
       // The tracker has been exceeded or disabled, so we just update the value (very fast)
        countsInc.increment(counter);
        return;
      }
      // We want to track changes to counters to maintain the sparse structure

      if (countsInc.incrementStatus(counter) == Incrementable.STATUS.wasZero) {
        // This is the first update of the counter, so we add it to the tracker
        final int oldTracksPos = tracksPos.getAndIncrement();
        if (oldTracksPos < tracksMax) {
          tracker[oldTracksPos] = counter;
        }
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
      if (oldValue == 0 && tracksPos.get() < tracksMax) {
        // This was the first update of the counter: Track it
        final int oldTracksPos = tracksPos.getAndIncrement();
        if (oldTracksPos < tracksMax) {
          tracker[oldTracksPos] = counter;
        }
      }
      break;
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
      // Update tracker
      final int oldTracksPos = tracksPos.getAndIncrement();
      if (oldTracksPos < tracksMax) {
        tracker[oldTracksPos] = 0;
      }
    }
  }

  /**
   * If the cut-off point has not been reached, clear time is linear to updated elements. If it has been reached,
   * clear time is linear to the total number of counts.
   */
  @Override
  public void clear() {
    if (tracksPos.get() >= tracksMax) {
      counts.clear();
    } else {
      for (int i = 0 ; i < tracksPos.get() ; i++) {
        counts.set(tracker[i],  0);
      }
    }
    explicitlyDisabled = false;
    tracksPos.set(0);
    missing.set(0);
    setContentKey(null);
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

    if (tracksPos.get() >= tracksMax || doNegative) { // Not sparse or very big (normally the same thing)
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
    callback.setOrdered(false);
    boolean filled = false;
    final int sparseMinValue = Math.max(minValue, 1);
    for (int t = 0 ; t < tracksPos.get() ; t++) {
      final int counter = tracker[t];
      long value = get(counter);
      if (counter >= start && counter <= end && value >= sparseMinValue) {
        filled |= callback.handle(counter, value);
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
    tracksPos.set(tracksMax);
    explicitlyDisabled = true;
  }

  @Override
  public boolean explicitlyDisabled() {
    return explicitlyDisabled;
  }

  @Override
  public String toString() {
    return "SparseCounterThreaded(counter=" + counts + ", counts=" + counts.size() + ", bpv=" + counts.getBitsPerValue()
        + ", trackers=" + tracksPos + "/" + tracksMax + ", maxCountForAny=" + maxCountForAny
        + ", minCountsForSparse=" + minCountsForSparse + ", fraction=" + fraction + ", explicitly disabled="
        + explicitlyDisabled + ')';
  }
}

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

import org.apache.lucene.util.packed.PackedInts;

/**
 * Re-usable sparse counter. Works on the same principle as {@link SparseCounterInt}, but uses
 * a PackedInts.Mutable instead of an int[] for counting. This means less memory overhead and in some cases a
 * performance overhead.
 * </p><p>
 * This class is optionally thread safe with regard to increments.
 * </p><p>
 * Tight-loop methods are final to help the compiler make optimizations.
 */
public class SparseCounterPacked implements ValueCounter {
  private final PackedInts.Mutable counts;  // One counter/tag
  private final int[] tracker; // Tracker not PackedInts.Mutable as it should be relatively small
  private final int tracksMax; // The maximum amount of trackers (tracker.length)
  // TODO: Remove the zero counter as it is replaced by missing
  private long zeroCounter = 0; // The counter at index 0 is special as it can exceed the maxValue of {@link #counts}

  private int tracksPos;       // The current amount of tracker entries. Setting this to 0 works as a tracker clear
  private int missing = 0;

  private final long maxCountForAny;    // The maximum count that it is possible to reach. Intended to PackedInts
  private final int minCountsForSparse; // The minimum amount of unique tags in order to perform sparse tracking at all
  private final double fraction;        // The fractional size of tracker (#trackers=#counts*fraction)
  private final long maxCountTracked;   // if any count reaches this number, it is not tracked anymore. For performance.
  private boolean explicitlyDisabled = false;

  private String contentKey = null;

  /**
   * @param counts            the number of counts to track.
   * @param maxCountForAny    the maximum amount any count can reach.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting.
   * @param maxCountTracked   if any count reaches this number, it is not tracked anymore. -1 disables this.
   *                          if specified, it is highly recommended to set this to 2^n-1, with 7, 255 and 65535
   *                          being the fastest.
   */
  public SparseCounterPacked(
      int counts, long maxCountForAny, int minCountsForSparse, double fraction, long maxCountTracked) {
    //this.counts = PackedInts.getMutable(counts, PackedInts.bitsRequired(maxCountForAny), PackedInts.FAST);
    this.counts = PackedInts.getMutable(counts, PackedInts.bitsRequired(
        maxCountTracked == -1 ? maxCountForAny : Math.min(maxCountTracked, maxCountForAny)), PackedInts.FAST);
    this.maxCountForAny = maxCountForAny;
    this.minCountsForSparse = minCountsForSparse;
    this.fraction = fraction;
    this.maxCountTracked = maxCountTracked;
    if (counts < minCountsForSparse) {
      tracksMax = 0;
      tracker = null;
    } else {
      tracksMax = (int) (counts * fraction);
//      tracker = PackedInts.getMutable(tracksMax, PackedInts.bitsRequired(counts), PackedInts.FAST);
      tracker = new int[tracksMax];
    }
  }

  @Override
  public ValueCounter createSibling() {
    SparseCounterPacked newCounter = new SparseCounterPacked(
        counts.size(), maxCountForAny, minCountsForSparse, fraction, maxCountTracked);
    newCounter.setContentKey(getContentKey());
    return newCounter;
  }

  /*
   * Constructs an ID which is unique for the given layout. Used for lookup of cached counters in
   * {@link SparseCounterPool}.
   */
  public static String createStructureKey(
      int counts, long maxCountForAny, int minCountForSparse, double fraction, long maxCountTracked) {
    return "SparseCounterPacked(counts" + counts + "maxCountForAny" + maxCountForAny
        + "minCountsForSparse" + minCountForSparse + "fraction" + fraction + "maxCountTracked" + maxCountTracked + ")";
  }

  /**
   * @return a key derived from the construction parameters. Used to group compatible SparseCounters.
   */
  @Override
  public String getStructureKey() {
    //return SparseCounter.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction);
    return SparseCounterPacked.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction, maxCountTracked);
  }

  /**
   * Increments the given counter.
   * @param counter the index of the counter to increment.
   */
  @Override
  public final void inc(int counter) {
    if (counter == 0) {
      zeroCounter++;
      if (zeroCounter == 1 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
      return;
    }

    final long oldValue = counts.get(counter);
    if (maxCountTracked == -1 || oldValue != maxCountTracked) {
      counts.set(counter, oldValue+1);
      if (oldValue == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    }
  }



  @Override
  public boolean hasThreadSafeInc() {
    return false;
  }
  /**
   * Increments the given counter with the given value.
   * If the value added is negative and the counter reaches 0, it will still be treated as an updated counter by the
   * sparse logic. This has no impact on functionality and will only result in a minuscule decrease of performance.
   * @param counter the index of the counter to increment.
   * @param value   the value to add to the counter.
   */
  private void inc(int counter, long value) {
    if (counter == 0) {
      zeroCounter += value;
      if (zeroCounter == value && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
      return;
    }

    long oldValue = counts.get(counter);
    if (maxCountTracked == -1) {
      counts.set(counter, oldValue+value);
      if (oldValue == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    } else {
      final int newVal = (int) (oldValue + value);
      counts.set(counter, newVal > maxCountTracked ? maxCountTracked : newVal);
      if (oldValue == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    }
  }

  @Override
  public void incMissing() {
    missing++;
  }

  @Override
  public long getMissing() {
    return missing;
  }

  @Override
  public final void set(int counter, long value) {
    if (counter == 0) {
      long oldValue = zeroCounter;
      zeroCounter = value;
      if (oldValue == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
      return;
    }

    long oldValue = counts.get(counter);
    if (maxCountTracked == -1) {
      counts.set(counter, value);
    } else {
      counts.set(counter, value > maxCountTracked ? maxCountTracked : value);
    }
    if (oldValue == 0 && value != 0 && tracksPos != tracksMax) {
      tracker[tracksPos++] = counter;
    }
  }

  /**
   * If the cut-off point has not been reached, clear time is linear to updated elements. If it has been reached,
   * clear time is linear to the total number of counts.
   */
  @Override
  public void clear() {
    if (tracksPos == tracksMax) {
      counts.clear();
    } else {
      for (int i = 0 ; i < tracksPos ; i++) {
        counts.set(tracker[i],  0);
      }
    }
    explicitlyDisabled = false;
    tracksPos = 0;
    zeroCounter = 0;
    missing = 0;
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
    return counter == 0 ? zeroCounter : counts.get(counter);
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

    if (tracksPos == tracksMax || doNegative) { // Not sparse or very big (normally the same thing)
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
    for (int t = 0 ; t < tracksPos ; t++) {
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
    tracksPos = tracksMax;
    explicitlyDisabled = true;
  }

  @Override
  public boolean explicitlyDisabled() {
    return explicitlyDisabled;
  }

  @Override
  public String toString() {
    return "SparseCounterPacked(counts=" + counts.size() + ", bpv=" + counts.getBitsPerValue()
        + ", trackers=" + tracksPos + "/" + tracksMax + ", maxCountForAny=" + maxCountForAny
        + ", minCountsForSparse=" + minCountsForSparse + ", fraction=" + fraction + ", explicitly disabled="
        + explicitlyDisabled + ')';
  }
}

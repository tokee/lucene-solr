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
package org.apache.solr.search.sparse.track;

import java.util.Arrays;

import org.apache.solr.search.sparse.SparseKeys;

/**
 * Re-usable sparse counter. Compared to using a simple int[] for counting, this implementation has
 * - Customizable cut-off point and corresponding memory overhead. 8% seems to be max before performance suffers.
 * - Somewhat slower updates up to the cut-off point. After that updates are about the same speed.
 * - Iteration speed linear to the number of updated elements up to the cut-off point. After that iteration
 *   speed is linear to the total counter size.
 * - Clear speed linear to the number of updated elements up to the cut-off point. After that clear is linear
 *   to the total counter size.
 * 
 * This class is not thread safe.
 * 
 * Tight-loop methods are final to help the compiler make optimizations.
 */
public class SparseCounterInt implements ValueCounter {
  // TODO: Switch to PackedInts when there is a reliable maxCountForAnyTag
//  private final PackedInts.Mutable counts;
//  private final PackedInts.Mutable tracker;
  private final int[] counts;  // One counter/tag
  private final int[] tracker; // Each entry contains the index into counts. indexes only occur once
  private final int tracksMax; // The maximum amount of trackers (tracker.length)

  private int tracksPos;       // The current amount of tracker entries. Setting this to 0 works as a tracker clear
  private long missing = 0;

  private final long maxCountForAny;    // The maximum count that it is possible to reach. Intended to PackedInts
  private final int minCountsForSparse; // The minimum amount of unique tags in order to perform sparse tracking at all
  private final double fraction;        // The fractional size of tracker (#trackers=#counts*fraction)
  private final int maxTracked;         // if any count reaches this number, it is not tracked anymore. For performance.
  private boolean explicitlyDisabled = false;

  private String contentKey = null;

  /**
   * Disables maxTracked (sets it to -1) as this is the expected use case with this tracker.
   * @param counts            the number of counts to track.
   * @param maxCountForAny    the maximum amount any count can reach.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting. -1 disabled this.
   */
  public SparseCounterInt(int counts, long maxCountForAny, int minCountsForSparse, double fraction) {
    this(counts, maxCountForAny, minCountsForSparse, fraction, SparseKeys.MAXTRACKED_DEFAULT);
  }

  /**
   * @param counts            the number of counts to track.
   * @param maxCountForAny    the maximum amount any count can reach.
   * @param minCountsForSparse count must be >= this in order for sparse counting to be activated.
   * @param fraction          the cut-off point between sparse and non-sparse counting.
   * @param maxTracked        if any count reaches this number, it is not tracked anymore.
   *                          Normally this will be -1 for this counter structure.
   */
  public SparseCounterInt(int counts, long maxCountForAny, int minCountsForSparse, double fraction, long maxTracked) {
    this.counts = new int[counts];
    this.maxCountForAny = maxCountForAny;
    this.minCountsForSparse = minCountsForSparse;
    this.fraction = fraction;
    this.maxTracked = maxTracked > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxTracked;
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
    SparseCounterInt newCounter = new SparseCounterInt(
        counts.length, maxCountForAny, minCountsForSparse, fraction, maxTracked);
    newCounter.setContentKey(getContentKey());
    return newCounter;
  }

  @Override
  public boolean hasThreadSafeInc() {
    return false;
  }

  /*
     * Constructs an ID which is unique for the given layout. Used for lookup of cached counters in
     * {@link SparseCounterPool}.
     */
  public static String createStructureKey(int counts, long maxCountForAny, int minCountForSparse, double fraction) {
    return "SparseCounterInt(counts" + counts + "maxCountForAny" + maxCountForAny
        + "minCountsForSparse" + minCountForSparse + "fraction" + fraction + "maxTracked=irrelevant)";
  }

  /**
   * @return a key derived from the construction parameters. Used to group compatible SparseCounters.
   */
  @Override
  public String getStructureKey() {
    //return SparseCounter.createStructureKey(counts.size(), maxCountForAny, minCountsForSparse, fraction);
    return SparseCounterInt.createStructureKey(counts.length, maxCountForAny, minCountsForSparse, fraction);
  }

  // TODO: Investigate is a proper hash & equals can be implemented by using structure & contentKey
/*  public int hashCode() {
    return getStructureKey().hashCode();
  }*/

  /**
   * Increments the given counter. This is slightly faster than {@link #inc(int, long)}.
   * @param counter the index of the counter to increment.
   */
  @Override
  public final void inc(int counter) {
    if (maxTracked == -1 || counts[counter] != maxTracked) { // maxTracked is final so hopefully JIT helps here
      if (counts[counter]++ == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    }
  }

  /**
   * Increments the given counter with the given value. If the value will always be 1, use {@link #inc(int)} instead.
   * If the value added is negative and the counter reaches 0, it will still be treated as an updated counter by the
   * sparse logic. This has no impact on functionality and will only result in a minuscule decrease of performance.
   * @param counter the index of the counter to increment.
   * @param value   the value to add to the counter.
   */
  private void inc(int counter, long value) {
    final long count = counts[counter];
    if (maxTracked == -1) {
      counts[counter] = (int) (count + value);
      if (count == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    } else {
      final int newVal = (int) (count + value);
      counts[counter] = newVal > maxTracked ? maxTracked : newVal;
      if (count == 0 && tracksPos != tracksMax) {
        tracker[tracksPos++] = counter;
      }
    }
    // Technically we should check for negative values as well as counter overflow
/*    long count = counts.get(counter);
    counts.set(counter, count+value);
    if (count == 0 && tracksPos != tracksMax) {
      tracker.set(tracksPos, counter);
      tracksPos++;
    }*/
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
    if (maxTracked == -1) {
      inc(counter, value - counts[counter]);
    } else {
      inc(counter, (value > maxTracked ? maxTracked : value) - counts[counter]);
    }
  }

  /**
   * If the cut-off point has not been reached, clear time is linear to updated elements. If it has been reached,
   * clear time is linear to the total number of counts.
   */
  @Override
  public void clear() {
    if (tracksPos == tracksMax) {
      Arrays.fill(counts, 0);
    } else {
      for (int i = 0 ; i < tracksPos ; i++) {
        counts[tracker[i]] = 0;
      }
    }
    explicitlyDisabled = false;
    tracksPos = 0;
    missing = 0;
    setContentKey(null);
  }

  /**
   * @return the absolute size of the counters.
   */
  @Override
  public int size() {
    return counts.length;
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
    return counts[counter];
  }

  // This code should be kept in sync with SparseCounterPacked.iterate
  @Override
  public boolean iterate(
      final int start, final int end, final int minValue, final boolean doNegative, final Callback callback) {
    if (start < 0 || end > size()) {
      throw new ArrayIndexOutOfBoundsException(String.format(
          "iterate(start=%d, end=%d, minValue=%d, callback) called on counter with size=%d",
          start, end, minValue, size()));
    }

    if (tracksPos == tracksMax || doNegative) { // Not sparse or very big (normally the same thing)
      callback.setOrdered(true);
      for (int counter = start ; counter < end ; counter++) {
        final long value = counts[counter];
        if (doNegative || value >= minValue) {
          callback.handle(counter, value);
        }
      }
      return false;
    }

    // Sparse
    callback.setOrdered(false);
    boolean filled = false;
    for (int t = 0 ; t < tracksPos ; t++) {
      final int counter = tracker[t];
      long value = counts[counter];
      if (counter >= start && counter <= end && value >= minValue) {
        filled |= callback.handle(counter, value);
      }
    }
    if (minValue == 0 && !filled) { // We need a second iteration to get enough 0-count values to fill the callback
      for (int counter = start ; counter < end ; counter++) {
        final long value = counts[counter];
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
    return "SparseCounterInt(counts=" + counts.length + ", trackers=" + tracksPos + "/" + tracksMax +
        ", maxCountForAny=" + maxCountForAny + ", minCountsForSparse=" + minCountsForSparse +
        ", fraction=" + fraction + ", explicitly disabled=" + explicitlyDisabled + ", maxTracked=" + maxTracked + ')';
  }
}

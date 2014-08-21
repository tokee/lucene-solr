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
package org.apache.solr.request.sparse;

import org.apache.solr.util.LongPriorityQueue;

/**
 * Keeps track of a fixed amount of counters, providing iteration of counters+values.
 * </p><p>
 * Fairly similar to {@link org.apache.lucene.util.packed.PackedInts.Mutable}
 * </p>
 */
public interface ValueCounter {

  /**
   * Increment the given counter with 1.
   * @param counter the index of the counter to increment.
   */
  void inc(int counter);

  /**
   * Increments the given counter with the given value. If the value will always be 1, use {@link #inc(int)} instead.
   * @param counter the index of the counter to increment.
   * @param delta   the value to add to the counter.
   */
  void inc(int counter, long delta);

  /**
   * Set the counter to the specific value
   * @param counter the index of the counter to increment.
   * @param value   the value to assign to the counter.
   */
  void set(int counter, long value);

  /**
   * @param counter the index of the counter to access.
   * @return the value for the given counter.
   */
  long get(int counter);

  /**
   * Set all values to 0.
   */
  void clear();

  /**
   * @return a key unique for the initial setup of the counter. Not changes when the counters are updated.
   */
  String getKey();

  /**
   * @return the number of individual counters, regardless of their value.
   */
  int size();

  /**
   * Iterate the counters, performing a callback for each counter that satisfies the given criteria. Use this
   * instead of outside login based on {@link #size()} and {@link #get(int)} as the iterate method can use
   * internal optimizations.
   * @param start    the start index of the counters to iterate, inclusive.
   * @param end      the end index of the counters to iterate, exclusive.
   * @param minValue the minimum value for the counter in order to trigger a callback.
   * @param doNegative signals whether the counts are the negative of the real counts.
   *                   Only affects iteration, not the values themselves.
   * @param callback a handler for matching counters.
   * @return true if the iteration was performed in an optimized manner, else false. Implementation-specific.
   */
  boolean iterate(int start, int end, int minValue, boolean doNegative, Callback callback);

  /**
   * If the caller has knowledge that the result set will be large, this method should be called.
   * The non-sparse flag will be removed on {@link #clear()}.
   */
  void disableSparseTracking();

  /**
   * @return true if {@link #disableSparseTracking()} has been called since last clear.
   */
  boolean explicitlyDisabled();

  /**
   * Used for (hopefully) efficient iteration of counters with {@link #iterate}.
   */
  public static interface Callback {
    /**
     * Called once before iteration starts.
     * @param isOrdered if true, {@link #handle(int, long)} will be called in counter order from lowest counter index
     *                  to highest counter index. If false, the order is not defined.
     */
    void setOrdered(boolean isOrdered);

    /**
     * Every counter/value pair matching the setup given to {@link #iterate} will result in a call to this method.
     * @param counter a counter matching the iterate criteria.
     * @param value   the value for the counter.
     */
    void handle(int counter, long value);
  }

  /**
   * Callback usable for most of Solr's faceting implementations to collect top-X ordinals in count order.
   */
  final class TopCallback implements Callback {
    private int min;
    private final int[] maxTermCounts;
    private final boolean doNegative;
    private final LongPriorityQueue queue;
    private boolean isOrdered = false;

    public TopCallback(int min, LongPriorityQueue queue) {
      this.maxTermCounts = null;
      this.min = min;
      this.doNegative = false;
      this.queue = queue;
    }

    public TopCallback(int[] maxTermCounts, int min, boolean doNegative, LongPriorityQueue queue) {
      this.maxTermCounts = maxTermCounts;
      this.min = min;
      this.doNegative = doNegative;
      this.queue = queue;
    }

    @Override
    public void setOrdered(boolean isOrdered) {
      this.isOrdered = isOrdered;
    }

    @Override
    public final void handle(final int counter, final long value) {
      final int c = (int) (doNegative ? maxTermCounts[counter] - value : value);
      if (isOrdered ? c > min : c>=min) {
        // NOTE: Using > only works when the results are delivered in order.
        // The ordered uses c>min rather than c>=min as an optimization because we are going in
        // index order, so we already know that the keys are ordered.  This can be very
        // important if a lot of the counts are repeated (like zero counts would be).
        // TODO: The order is not guaranteed with SparseCounter

        // smaller term numbers sort higher, so subtract the term number instead
        final long pair = (((long)c)<<32) + (Integer.MAX_VALUE - counter);
        //boolean displaced = queue.insert(pair);
        if (queue.insert(pair)) min=(int)(queue.top() >>> 32);
      }
    }
  }
}

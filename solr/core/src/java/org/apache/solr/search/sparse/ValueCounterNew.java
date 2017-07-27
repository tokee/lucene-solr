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
package org.apache.solr.search.sparse;

import org.apache.solr.util.LongPriorityQueue;

/**
 * Keeps track of a fixed amount of buckets, providing iteration of buckets+values.
 * Fairly similar to {@link org.apache.lucene.util.packed.PackedInts.Mutable}
 */
public interface ValueCounterNew {

  /**
   * Increment special counter for missing values.
   */
  void incMissing();
  long getMissing();

  /**
   * Increment the given bucket with 1.
   * @param bucket the index of the bucket to increment.
   */
  void inc(long bucket);

  /**
   * Increment the bucket at the given index by 1.
   * If the value overflows, 0 must be stored at the index.
   * @param index the index for the value to increment.
   * @return the state of the bucket before and after the operation.
   */
  default STATUS incrementStatus(long index) {
    final long oldVal = get(index);
    set(index, oldVal+1);
    // No handling of overflow as default as Long.MAX_VALUE is practically unreachable
    return oldVal == 0 ? STATUS.wasZero : STATUS.ok;
  }
  enum STATUS { // All are mutually exclusive as buckets always goes to > 0
    wasZero,      // The counter was zero before increment
    ok,           // Increment from non-zero without overflow
    overflowed    // Overflow occurred due to the increment
  }

  /**
   * Set the buckety to the specific value
   * @param bucket the index of the buckety to increment.
   * @param value   the value to assign to the bucket.
   */
  void set(long bucket, long value);

  /**
   * @param bucket the index of the bucket to access.
   * @return the value for the given bucket.
   */
  long get(long bucket);

  /**
   * Set all buckets except structureKey to 0. This includes contentKey.
   */
  void clear();

  /**
   * @return a key unique for the initial setup of the counter. Does not not change when the counters are updated.
   */
  String getStructureKey();

  /**
   * @return a key unambigiously designating the state of the counters or null if the ValueCounterNew is empty or
   * should be cleaned. Used for cache lookup.
   */
  String getContentKey();

  /**
   * @param contentKey unambigiously designates the state of the counters or null if the ValueCounterNew should be cleaned.
   * Used for cache lookup.
   */
  void setContentKey(String contentKey);

  /**
   * @return the number of individual counters, regardless of their value.
   */
  int size();

  /**
   * Iterate the counters, performing a callback for each counter that satisfies the given criteria. Use this
   * instead of outside login based on {@link #size()} and {@link #get(long)} as the iterate method can use
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
   * @return an empty ValueCounterNew with the same setup as this, potentially sharing underlying support structures.
   */
  ValueCounterNew createSibling();

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
     * @return true if the call resulted in another  value being dropped from the receiving structure. This is used for optimization purposes
     * when iterating with minCount==0.
     */
    boolean handle(int counter, long value);
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

    /**
      * Creates a basic callback where only the values >= min are considered.
      * @param min      the starting min value.
      * @param queue   the destination of the values of the counters.
      */
     public TopCallback(int min, LongPriorityQueue queue) {
       this.maxTermCounts = null;
       this.min = min;
       this.doNegative = false;
       this.queue = queue;
     }

     /**
       * A callback that handles "negative counts". If doNegative is true, the value of the counter is subtracted from
      * the corresponding entry in maxTermCounts. This is used to speed up processing of large facet results,
       * @param maxTermCounts 1:1 correspondence with the counter list.
       * @param min           the starting min value.
       * @param doNegative    if true, the value of a counter is considered to be
      *                       {@code maxTermCounts[index]-counter[index]}.
       * @param queue         the destination of the values of the counters.
       */
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
    public final boolean handle(final int counter, final long value) {
      final int c = (int) (doNegative ? maxTermCounts[counter] - value : value);
      if (isOrdered ? c > min : c>=min) {
        // NOTE: Using > only works when the results are delivered in order.
        // The ordered uses c>min rather than c>=min as an optimization because we are going in
        // index order, so we already know that the keys are ordered.  This can be very
        // important if a lot of the counts are repeated (like zero counts would be).

        // smaller term numbers sort higher, so subtract the term number instead
        final long pair = (((long)c)<<32) + (Integer.MAX_VALUE - counter);
        //boolean displaced = queue.insert(pair);
        if (queue.insert(pair)) {
          min=(int)(queue.top() >>> 32);
          return true;
        }
      }
      return false;
    }
  }
}

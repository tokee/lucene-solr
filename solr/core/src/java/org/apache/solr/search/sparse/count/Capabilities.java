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
package org.apache.solr.search.sparse.count;

/**
 * Capability-signalling used by counters and factories of counters.
 */
public interface Capabilities {
  /**
   * @return 
   */
  String id();

  enum STARTUP_REQUIREMENT {
    /* The counter can be created directly */
    none,
    /* The maximum value for any bucket must be provided */
    maxValue,
    /* A full histogram for all values must be provided, with the histogram entries going from 0-bit to 63-bit */
    valueHistogram,
    /* An iterator for all maximum bucket sizes must be provided */
    allValues}


  /**
   * @return true if mutating calls to the counter are thread-safe.
   */
  boolean isThreadSafe();

  /**
   * @return true if the counter supports sparse updates.
   */
  boolean isSparse();

  /**
   * Primarily used by counter factories to determine counter candidates.
   * @return the maximum counter value.
   */
  long maxCounterValue();

  /**
   * Primarily used by counter factories to determine counter candidates.
   * @param averageBPV the average bits per value that the counter entries takes up. If this value is unknown,
   *                   a reasonable guess, such as 31 log2((Integer.MAX_VALUE)) should be used.
   * @return the maximum amount of entries in this counter. Usually about 2^31.
   */
  long maxEntries(int averageBPV);

  /**
   * @return the amount of information needed in order to create the first counter of this type.
   */
  STARTUP_REQUIREMENT startupRequirements();
}

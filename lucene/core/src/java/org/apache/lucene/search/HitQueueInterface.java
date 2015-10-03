package org.apache.lucene.search;

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

import java.util.Iterator;

public interface HitQueueInterface {
  /**
   * @return the number of elements in the queue.
   */
  int size();

  /**
   * @return the maximum number of elements that can be in the queue at any given time.
   */
  int capacity();

  /**
   * Add an element to the queue, potentially pushing another element out.
   * </p><p>
   * Note: If a ScoreDoc is not readily available, use the method {@link #add(int, float)} instead.
   * @param element the element to add.
   * @return the previous element if the given element pushd one from the queue, else null.
   */
  ScoreDoc insert(ScoreDoc element);

  /**
   * Add an element to the queue, potentially pushing another element out.
   * @param docID the global ID for the document.
   * @param score the score for the document.
   */
  void insert(int docID, float score);

  /**
   * Clears the queue.
   */
  void clear();

  /**
   * @return true if the queue is empty.
   */
  boolean isEmpty();

  /**
   * An iterator for the content of the queue. When the iterator is depleted, the queue will be empty.
   * </p><p>
   * Each parameters that is set to true allow special implementations to do faster processing.
   * @param unordered if true, the order of the delivered ScoreDocs is not guaranteed.
   * @param reuseScoreDoc if true, the returned ScoreDoc is reused when calling {@code next()} on the iterator.
   *                      If so, it is the responsibility of the caller to copy the values from the delivered scoreDoc
   *                      before calling {@code next()} and not use the deliveres ScoreDoc for anything else.
   * @return an iterator over the queue content.
   */
  Iterator<ScoreDoc> getFlushingIterator(boolean unordered, boolean reuseScoreDoc);

}

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

import java.util.Arrays;

import org.apache.lucene.util.PriorityQueue;

/**
 * Special purpose class for collecting tuples of score (float), docID (int) and shardIndex (int), acting as direct
 * replacement for {@link org.apache.lucene.search.HitQueue}.
 * </p><p>
 * The design principle for the HitQueueArray is the use of atomic arrays instead of Object arrays. For larger
 * queues (the value of "large" has yet to be determined) this means faster processing and substantially less
 * garbage collections.
 * </p><p>
 * It is technically possible to pack a tuple into a single long: The float value takes 32 bits and maxDocID is < 2^31
 * for the full index. The problem with this is that the docID _and_ the segmentIndex must be preserved and from the
 * perspective of the HitQueueArray there is no correspondence between docID and segmentIndex. If this is to be done,
 * the use of HitQueueArray would require a function {@code f(segmentIndex, segmentDocID) → globalDocID} and vice versa.
 * </p>
 **/
public class HitQueueArray {
  private final int maxSize;

  private final float[] scores;     // document score
  private final long[] docSegments; // segmentIndex << 32 | docID
  private int size = 0;

  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public HitQueueArray(int size) {
    maxSize = size+1;
    scores = new float[size+1];
    docSegments = new long[size+1];
    clear(); // Init with negative infinity
  }

  private final ScoreDoc insertScoreDocOld = new ScoreDoc(0, 0f);
  public ScoreDoc insertWithOverflow(ScoreDoc element) {
    if (size < maxSize-1) {
      assign(element, ++size);
      upHeap();
      element.score = Float.NEGATIVE_INFINITY; // Unnecessary reset?
      return element;
    } else if (size > 0 && !lessThan(element, assign(1, insertScoreDocOld))) {
      assign(element, 1);
      downHeap();
      return assign(insertScoreDocOld, element);
    } else {
      return element;
    }
  }

  private final ScoreDoc upHeapOld = new ScoreDoc(0, 0f);
  private void upHeap() {
    int i = size;
    assign(i, upHeapOld);
    int j = i >>> 1;
    while (j > 0 && lessThan(upHeapOld, j)) {
      assign(j, i); // shift parents down
      i = j;
      j = j >>> 1;
    }
    assign(upHeapOld, i);
  }

  private final ScoreDoc downHeapOld = new ScoreDoc(0, 0f);
  private void downHeap() {
    int i = 1;
    assign(i, downHeapOld);    // save top node
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(k, j)) {
      j = k;
    }
    while (j <= size && lessThan(j, downHeapOld)) {
      assign(j, i);            // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(k, j)) {
        j = k;
      }
    }
    assign(downHeapOld, i);            // install saved node
  }

  public final void clear() {
    size = 0;
    Arrays.fill(scores, Float.NEGATIVE_INFINITY); // Do we even need to do this when size == =?
    // No need for clearing docSegments as negative infinity in scores handles it all
  }

  public int size() {
    return size;
  }

  public final ScoreDoc top(ScoreDoc reuse) {
    if (size() == 0) {
      return null;
    }
    if (reuse == null) {
      reuse = new ScoreDoc(0, 0f);
    }
    assign(1, reuse);
    return reuse;
  }

  /** Removes and returns the least element of the PriorityQueue in log(size) time. */
  public final ScoreDoc pop(ScoreDoc reuse) {
    if (size == 0) {
      return null;
    }
    if (reuse == null) {
      reuse = new ScoreDoc(0, 0f);
    }
    assign(1, reuse);         // save first value
    assign(size, 1);          // move last to first
    scores[size] = Float.NEGATIVE_INFINITY; // should not be needed?
    docSegments[size] = 0;                  // should not be needed?
    size--;
    downHeap();               // adjust heap
    return reuse;
  }

  private void assign(int fromIndex, int toIndex) {
    scores[toIndex] = scores[fromIndex];
    docSegments[toIndex] = docSegments[fromIndex];
  }
  private ScoreDoc assign(int fromIndex, ScoreDoc toScoreDoc) {
    toScoreDoc.score = scores[fromIndex];
    toScoreDoc.doc = (int)(docSegments[fromIndex] >>> 32);
    toScoreDoc.shardIndex = (int)docSegments[fromIndex]; // Casting removed upper bits
    return toScoreDoc;
  }
  private void assign(ScoreDoc fromScoreDoc, int toIndex) {
    scores[toIndex] = fromScoreDoc.score;
    docSegments[toIndex] = ((long)fromScoreDoc.doc) << 32 | ((long)fromScoreDoc.shardIndex);
  }
  private ScoreDoc assign(ScoreDoc fromScoreDoc, ScoreDoc toScoreDoc) {
    toScoreDoc.score = fromScoreDoc.score;
    toScoreDoc.doc = fromScoreDoc.doc;
    toScoreDoc.shardIndex = fromScoreDoc.shardIndex;
    return toScoreDoc;
  }

  @SuppressWarnings("FloatingPointEquality")
  protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
   return hitA.score == hitB.score ? hitA.doc > hitB.doc : hitA.score < hitB.score;
  }

  @SuppressWarnings("FloatingPointEquality")
  protected final boolean lessThan(int index, ScoreDoc hitA) {
   return scores[index] == hitA.score ? (int)(docSegments[index] >>> 32) > hitA.doc : scores[index] < hitA.score;
  }
  @SuppressWarnings("FloatingPointEquality")
  protected final boolean lessThan(ScoreDoc hitA, int index) {
   return hitA.score == scores[index] ? hitA.doc > (int)(docSegments[index] >>> 32) : hitA.score < scores[index];
  }
  @SuppressWarnings("FloatingPointEquality")
  protected final boolean lessThan(int indexA, int indexB) {
   return scores[indexA] == scores[indexB] ?
       docSegments[indexA] > docSegments[indexB] :
       scores[indexA] < scores[indexB];
  }
}

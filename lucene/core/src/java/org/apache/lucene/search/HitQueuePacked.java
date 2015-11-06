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
import java.util.Iterator;

/**
 * Special purpose class for collecting tuples of score (float), docID (int) and shardIndex (int), acting as direct
 * replacement for {@link HitQueue}.
 * </p><p>
 * The design principle for the HitQueueArray is the use of atomic arrays instead of Object arrays. For larger
 * queues (the value of "large" has yet to be determined) this means faster processing and substantially less
 * garbage collections.
 * </p><p>
 * Note: This implementation ignores the shardIndex from ScoreDoc.
 * </p><p>
 * Warning: This class is used primarily for experimentations with performance.
 * Correctness of the implementation has not been properly unit tested!
 * </p><p>
 * Observations:<br/>
 * HitQueuePacked outperforms HitQueueArray in most cases and has the added bonus of requiring 1/3 less heap.
 *
 **/
public class HitQueuePacked implements HitQueueInterface {
  private final int maxSize;

  // TODO: Switch to a gradually growing (up to max) scheme as extending is fairly cheap with System.arrayCopy
  private final long[] elements;
  private int size = 0;
  private boolean dirty = false;

  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public HitQueuePacked(int size){
    maxSize = size+1;
    elements = new long[maxSize];
    clear(); // Init with negative infinity
  }

  private final ScoreDoc insertScoreDocOld = new ScoreDoc(0, 0f);
  @Override
  public ScoreDoc insert(ScoreDoc newScoreDoc) {
    if (size < maxSize-1) {
      assign(newScoreDoc, ++size);
      dirty = true;
//      upHeap();
      return newScoreDoc;
    } else if (size > 0 && !lessThan(newScoreDoc, assign(1, insertScoreDocOld))) {
      orderHeap();
      assign(newScoreDoc, 1);
      downHeap();
      return assign(insertScoreDocOld, newScoreDoc);
    } else {
      return newScoreDoc;
    }
  }
  @Override
  public void insert(int docID, float score) {
    if (size < maxSize-1) {
      assign(docID, score, ++size);
      dirty = true;
    } else if (size > 0 && !lessThan(docID, score, 1)) {
      orderHeap();
      assign(docID, score, 1);
      downHeap();
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

  //private final ScoreDoc downHeapOld = new ScoreDoc(0, 0f);
  private void downHeap() {
    int i = 1;
    final long downHeapOld = elements[i];
//    assign(i, downHeapOld);    // save top node
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
    elements[i] = downHeapOld;
//    assign(downHeapOld, i);            // install saved node
  }

  @Override
  public final void clear() {
    size = 0;
    dirty = false;
//    Arrays.fill(scores, Float.NEGATIVE_INFINITY); // Do we even need to do this when size == =?
    // No need for clearing docIDs as negative infinity in scores handles it all
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int capacity() {
    return maxSize;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public Iterator<ScoreDoc> getFlushingIterator(boolean unordered, boolean reuseScoreDoc) {
    boolean directIteration = unordered;
    if (!unordered && dirty) { // Order needed, but no previous order so we merge sort (faster than heap create + empty)
      if (size() > 1) {
        Arrays.sort(elements, 1, size+1);
      }
      directIteration = true;
    }
    return directIteration ? new HQIteratorDirect(reuseScoreDoc) : new HQIterator(reuseScoreDoc);
  }

  public final ScoreDoc top(ScoreDoc reuse) {
    if (size() == 0) {
      return null;
    }
    if (reuse == null) {
      reuse = new ScoreDoc(0, 0f);
    }
    orderHeap(); // TODO: Check if top is called often with changing heap
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
    orderHeap();
    assign(1, reuse);         // save first value
    assign(size, 1);          // move last to first
//    scores[size] = Float.NEGATIVE_INFINITY; // should not be needed?
//    docIDs[size] = 0;                  // should not be needed?
    size--;
    downHeap();               // adjust heap
    return reuse;
  }

  private void orderHeap() {
    if (!dirty) {
      return;
    }
    heapSort(1, size);
    dirty = false;
  }

  private void heapSort(int from, int to) {
    if (to - from <= 1) {
      return;
    }
    heapify(from, to);
    for (int end = to - 1; end > from; --end) {
      swap(from, end);
      siftDown(from, from, end);
    }
  }

  private void heapify(int from, int to) {
    for (int i = heapParent(from, to - 1); i >= from; --i) {
      siftDown(i, from, to);
    }
  }

  private void siftDown(int i, int from, int to) {
    for (int leftChild = heapChild(from, i); leftChild < to; leftChild = heapChild(from, i)) {
      final int rightChild = leftChild + 1;
      if (lessThan(i, leftChild)) {
        if (rightChild < to && lessThan(leftChild, rightChild)) {
          swap(i, rightChild);
          i = rightChild;
        } else {
          swap(i, leftChild);
          i = leftChild;
        }
      } else if (rightChild < to && lessThan(i, rightChild)) {
        swap(i, rightChild);
        i = rightChild;
      } else {
        break;
      }
    }
  }

  private int heapParent(int from, int i) {
    return ((i - 1 - from) >>> 1) + from;
  }

  private int heapChild(int from, int i) {
    return ((i - from) << 1) + 1 + from;
  }

  private void assign(int fromIndex, int toIndex) {
    elements[toIndex] = elements[fromIndex];
  }
  private void swap(int indexA, int indexB) {
    long temp = elements[indexA];
    elements[indexA] = elements[indexB];
    elements[indexB] = temp;
  }
  private ScoreDoc assign(int fromIndex, ScoreDoc toScoreDoc) {
    elementToScoreDoc(elements[fromIndex], toScoreDoc);
    return toScoreDoc;
  }
  private void assign(ScoreDoc fromScoreDoc, int toIndex) {
    elements[toIndex] = scoreDocToElement(fromScoreDoc);
  }
  private void assign(int docID, float score, int index) {
    elements[index] = valuesToElement(docID, score);
  }


  private ScoreDoc assign(ScoreDoc fromScoreDoc, ScoreDoc toScoreDoc) {
    toScoreDoc.score = fromScoreDoc.score;
    toScoreDoc.doc = fromScoreDoc.doc;
    toScoreDoc.shardIndex = fromScoreDoc.shardIndex;
    return toScoreDoc;
  }

  /* Core conversion methods. Note the special handling of docID to ensure proper sort order */
  private long valuesToElement(int docID, float score) {
    return (((long)Float.floatToRawIntBits(score)) << 32) | (Integer.MAX_VALUE-docID);
  }
  private long scoreDocToElement(ScoreDoc scoreDoc) {
    return (((long)Float.floatToRawIntBits(scoreDoc.score)) << 32) | (Integer.MAX_VALUE-scoreDoc.doc);
  }
  private void elementToScoreDoc(long element, ScoreDoc scoreDoc) {
    scoreDoc.score = Float.intBitsToFloat((int) (element >>> 32));
    scoreDoc.doc = Integer.MAX_VALUE-((int) element);
  }

  @SuppressWarnings("FloatingPointEquality")
  protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
   return hitA.score == hitB.score ? hitA.doc > hitB.doc : hitA.score < hitB.score;
  }

  private boolean lessThan(int docID, float score, int index) {
    return valuesToElement(docID, score) < elements[index];
  }
  protected final boolean lessThan(int index, long element) {
    return elements[index] < element;
  }
  protected final boolean lessThan(int index, ScoreDoc hitA) {
    return elements[index] < scoreDocToElement(hitA);
/*    float firstScore = Float.intBitsToFloat((int) (elements[index] >>> 32));
    return Float.intBitsToFloat((int) (elements[index] >>> 32)) == hitA.score ?
        ((int)elements[index]) > hitA.doc :
        firstScore < hitA.score;*/
  }
  protected final boolean lessThan(ScoreDoc hitA, int index) {
    return scoreDocToElement(hitA) < elements[index];
/*    float secondScore = Float.intBitsToFloat((int) (elements[index] >>> 32));
    return hitA.score == secondScore ? hitA.doc > ((int)elements[index]) : hitA.score < secondScore;*/
  }
  protected final boolean lessThan(int indexA, int indexB) {
    return elements[indexA] < elements[indexB];
  }

  // Assumes the heap is ordered
  private class HQIterator implements Iterator<ScoreDoc> {
    private final ScoreDoc scoreDoc = new ScoreDoc(0, 0f);
    private final boolean reuse;
    public HQIterator(boolean reuseScoreDoc) {
      reuse = reuseScoreDoc;
    }

    @Override
    public boolean hasNext() {
      return !isEmpty();
    }

    @Override
    public ScoreDoc next() {
      return pop(reuse ? scoreDoc : null);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not possible as calling next() already removes");
    }
  }

  // Delivers the values in the array order
  private class HQIteratorDirect implements Iterator<ScoreDoc> {
    private final ScoreDoc scoreDoc = new ScoreDoc(0, 0f);
    private final boolean reuse;
    private int index = 1;
    public HQIteratorDirect(boolean reuseScoreDoc) {
      reuse = reuseScoreDoc;
    }

    @Override
    public boolean hasNext() {
      return index <= size;
    }

    @Override
    public ScoreDoc next() {
      if (reuse) {
        assign(index++, scoreDoc);
        return scoreDoc;
      }
      ScoreDoc fresh = new ScoreDoc(0, 0f);
      assign(index++, fresh);
      return fresh;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not possible as calling next() already removes");
    }
  }

}

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
 * replacement for {@link org.apache.lucene.search.HitQueue}.
 * </p><p>
 * This implementation is just a thin wrapper for {@link BHeap}.
 * </p><p>
 * Note: This implementation ignores the shardIndex from ScoreDoc.
 * </p><p>
 * Warning: This class is used primarily for experimentations with performance.
 * Correctness of the implementation has not been properly unit tested!
 * </p><p>
 * Observations:<br/>
 *
 **/
public class HitQueuePackedBHeap extends BHeap implements HitQueueInterface {
  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public HitQueuePackedBHeap(int size, int exponent){
    super(size, exponent);
  }

  @Override
  public ScoreDoc insert(ScoreDoc element) {
    insert(scoreDocToElement(element));
    return element;
  }

  @Override
  public void insert(int docID, float score) {
    insert(valuesToElement(docID, score));
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

  @Override
  public Iterator<ScoreDoc> getFlushingIterator(boolean unordered, boolean reuseScoreDoc) {
    return new HQIterator(reuseScoreDoc);
  }

  // Assumes the heap is ordered
  private class HQIterator implements Iterator<ScoreDoc> {
    private final ScoreDoc scoreDoc;
    private final boolean reuse;

    public HQIterator(boolean reuseScoreDoc) {
      reuse = reuseScoreDoc;
      scoreDoc = reuse ? new ScoreDoc(0, 0f) : null;
    }

    @Override
    public boolean hasNext() {
      return !isEmpty();
    }

    @Override
    public ScoreDoc next() {
      long element = pop();
      if (reuse) {
        elementToScoreDoc(element, scoreDoc);
        return scoreDoc;
      }
      ScoreDoc scoreDoc = new ScoreDoc(0, 0f);
      elementToScoreDoc(element, scoreDoc);
      return scoreDoc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not possible as calling next() already removes");
    }

  }


}

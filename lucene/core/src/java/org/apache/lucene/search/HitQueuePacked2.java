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

import org.apache.lucene.util.PriorityQueueLong;

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
public class HitQueuePacked2 extends PriorityQueueLong<ScoreDoc> implements HitQueueInterface {

  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public HitQueuePacked2(int size){
    super(size);
  }

  @Override
  public void insert(int docID, float score) {
    if (size < maxSize-1) {
      elements[++size] = serialize(docID, score);
      dirty = true;
    } else if (size > 0) {
      long element = serialize(docID, score);
      if(!lessThan(element, elements[1])) {
        orderHeap();
        elements[1] = element;
        downHeap();
      }
    }
  }

  @Override
  protected ScoreDoc deserialize(long block, ScoreDoc reuse) {
    if (reuse == null) {
      return new ScoreDoc(Integer.MAX_VALUE-((int) block), Float.intBitsToFloat((int) (block >>> 32)));
    }
    reuse.score = Float.intBitsToFloat((int) (block >>> 32));
    reuse.doc = Integer.MAX_VALUE-((int) block);
    return reuse;
  }

  @Override
  protected long serialize(ScoreDoc scoreDoc) {
    return (((long)Float.floatToRawIntBits(scoreDoc.score)) << 32) | (Integer.MAX_VALUE-scoreDoc.doc);
  }

  private long serialize(int docID, float score) {
    return (((long)Float.floatToRawIntBits(score)) << 32) | (Integer.MAX_VALUE-docID);
  }

  @Override
  @SuppressWarnings("FloatingPointEquality")
  public final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
   return hitA.score == hitB.score ? hitA.doc > hitB.doc : hitA.score < hitB.score;
  }

  @Override
  public boolean lessThan(long blockA, long blockB) {
    return blockA < blockB;
  }

  @Override
  protected boolean lessThan(ScoreDoc scoreDoc, long block) {
    return ((((long)Float.floatToRawIntBits(scoreDoc.score)) << 32) | (Integer.MAX_VALUE-scoreDoc.doc)) < block;
  }
}

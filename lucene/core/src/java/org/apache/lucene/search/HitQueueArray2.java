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

import org.apache.lucene.util.PriorityQueueInts;

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
 **/
public class HitQueueArray2 extends PriorityQueueInts<ScoreDoc> implements HitQueueInterface {
  private final int shardID; // -1 means shardIDs are stored in the elements
  private final ScoreDoc tmpDoc;

  /**
   * Creates a queue for ScoreDocs.
   *
   * @param size maximum size of the queue.
   */
  public HitQueueArray2(int size) {
    super(size, 2);
    shardID = -1;
    tmpDoc = new ScoreDoc(0, 0f, shardID);
  }

  /**
   * Creates a specialized queue for ScoreDocs where the shardID is always the same.
   * This saves 1/3 of used memory and increases performance a bit.
   *
   * @param size maximum size of the queue.
   */
  public HitQueueArray2(int size, int shardID) {
    super(size, 3);
    this.shardID = shardID;
    tmpDoc = new ScoreDoc(0, 0f, shardID);
  }

  @Override
  public void insert(int docID, float score) {
    tmpDoc.score = score;
    tmpDoc.doc = docID;
    insert(tmpDoc);
  }

  @Override
  protected ScoreDoc deserialize(int[] ints, int offset, ScoreDoc reuse) {
    if (reuse == null) {
      return new ScoreDoc(ints[1], Float.intBitsToFloat(ints[0]), shardID == -1 ? -1 : ints[2]);
    }
    reuse.doc = ints[1];
    reuse.score = Float.intBitsToFloat(ints[0]);
    reuse.shardIndex = shardID == -1 ? -1 : ints[2];
    return reuse;
  }

  @Override
  protected void serialize(ScoreDoc element, int[] ints, int offset) {
    ints[0] = Float.floatToRawIntBits(element.score);
    ints[1] = element.doc;
    if (shardID != -1) {
      ints[2] = element.shardIndex;
    }
  }

  public int[] toElement(ScoreDoc scoreDoc) {
    int[] element = new int[elementSize];
    element[0] = Float.floatToRawIntBits(scoreDoc.score);
    element[1] = scoreDoc.doc;
    if (shardID != -1) {
      element[2] = scoreDoc.shardIndex;
    }
    return element;
  }

  @SuppressWarnings("FloatingPointEquality")
  @Override
  public boolean lessThan(ScoreDoc elementA, ScoreDoc elementB) {
    return elementA.score == elementB.score ? elementA.doc > elementB.doc : elementA.score < elementB.score;
  }

  @Override
  public boolean lessThan(int[] elementA, int offsetA, int[] elementB, int offsetB) {
    // The raw bits representation of Floats are order comparable for values >= 0 and all scores are positive
    return elementA[offsetA] == elementB[offsetB] ?
        elementA[offsetA+1] > elementB[offsetB+1] :
        elementA[offsetA] == elementB[offsetB];
  }
}

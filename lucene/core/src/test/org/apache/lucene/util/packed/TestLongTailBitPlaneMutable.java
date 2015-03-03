package org.apache.lucene.util.packed;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;

import java.util.Locale;
import java.util.Random;

@Slow
public class TestLongTailBitPlaneMutable extends LuceneTestCase {

  private final static int M = 1048576;

  public void testSizeEstimate() {
    LongTailBitPlaneMutable bpm = new LongTailBitPlaneMutable(shift(TestLongTailMutable.getLinksHistogram(), 519*M));
    System.out.println("Estimated memory for LongTailBitPlaneMutable: " + bpm.ramBytesUsed()/M + "MB");
  }

  public void testTrivialSpeed() {
    final long UPDATES = 100*M;
    LongTailBitPlaneMutable bpm = new LongTailBitPlaneMutable(
        shift(TestLongTailMutable.getLinksHistogram(), 519*M), 10000);
    final long start = System.nanoTime();
    Random random = new Random(87);
    for (long update = 0 ; update < UPDATES ; update++) {
      int index = random.nextInt(519*M);
      bpm.set(index, bpm.get(index)+1);
      if ((update & 0xfff) == 0xfff) {
        double ups = 1.0 * update / ((System.nanoTime()-start)/M);
        System.out.println(String.format("Update %4d, %4.2f updates/ms", update, ups));
      }
    }
  }

  private static long[] shift(long[] histogram, long valueCount) {
    System.arraycopy(histogram, 0, histogram, 1, histogram.length-1);
    histogram[0] = valueCount;
    return histogram;
  }

}

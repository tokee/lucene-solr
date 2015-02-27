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

@Slow
public class TestLongTailMutable extends LuceneTestCase {

  private final static int M = 1048576;
  public void testEstimate() {
    LongTailMutable.Estimate estimate = new LongTailMutable.Estimate(100*M, getSampleHistogram());
    for (int tailBPV = 0 ; tailBPV < 64 ; tailBPV++) {
      System.out.println(String.format("tailBPV=%2d fraction=%4.2f", tailBPV, estimate.getFractionEstimate(tailBPV)));
    }
  }

  private long[] getSampleHistogram() {
    return new long[] {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 32-63
        0, 0, 0, 0, 0, 0, 0, 0, // 24-31
        0, 0, 5, 99, 335, 619, 643, 2363, // 16-23
        5630, 26037, 74934, 180056, 324288, 541195, 875950, 1439165, // 8-15
        2317588, 3830153, 7074694, 12677476, 22636130, 37200810, 57336093, 324916865 // 0-7
    };
  }

}

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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.junit.Ignore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

@Slow
public class TestCompactIntMap extends LuceneTestCase {

  public void testSingle() {
    CompactIntMap cim = new CompactIntMap(2, 8);
    cim.put(7, 87);
    assertEquals("Extraction for key 7 should work", 87, cim.get(7));
  }

  public void testDualDifferentBuckets() {
    CompactIntMap cim = new CompactIntMap(2, 8);
    cim.put(7, 87); cim.put(2, 86);
    assertEquals("Extraction for key 7 should work", 87, cim.get(7));
    assertEquals("Extraction for key 2 should work", 86, cim.get(2));
  }

  public void testDualSameBucket() {
    CompactIntMap cim = new CompactIntMap(2, 8);
    cim.put(7, 87); cim.put(3, 86);
    assertEquals("Extraction for key 7 should work", 87, cim.get(7));
    assertEquals("Extraction for key 3 should work", 86, cim.get(3));
  }

}

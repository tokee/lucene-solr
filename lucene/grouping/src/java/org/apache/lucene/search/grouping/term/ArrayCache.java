package org.apache.lucene.search.grouping.term;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Proof of concept cache for primitive arrays.
 */
public class ArrayCache {
  private final int maxArrays; // Memory would be better, but POC!
  private final float overAllocationFactor = 1.1f; 
  private int neededLength = 0;
  private int currentArrays = 0;
  
  private List<float[]> floats = new ArrayList<>(); 
  private List<double[]> doubles = new ArrayList<>(); 
  private List<int[]> ints = new ArrayList<>(); 
  private List<long[]> longs = new ArrayList<>(); 
  
  public ArrayCache(int maxArrays, int maxDoc) {
    this.maxArrays = maxArrays;
    this.neededLength = maxDoc;
  }

  public synchronized float[] getFloats() {
    if (floats.isEmpty()) {
      return new float[(int) (neededLength * overAllocationFactor)];
    }
    currentArrays--;
    return floats.remove(floats.size()-1);
  }
  
  public synchronized double[] getDoubles() {
    if (doubles.isEmpty()) {
      return new double[(int) (neededLength * overAllocationFactor)];
    }
    currentArrays--;
    return doubles.remove(doubles.size()-1);
  }
  
  public synchronized int[] getInts() {
    if (ints.isEmpty()) {
      return new int[(int) (neededLength * overAllocationFactor)];
    }
    currentArrays--;
    return ints.remove(ints.size()-1);
  }
  
  public synchronized long[] getLongs() {
    if (longs.isEmpty()) {
      return new long[(int) (neededLength * overAllocationFactor)];
    }
    currentArrays--;
    return longs.remove(longs.size()-1);
  }
  
  // TODO: Releases should only be synchronized on the actual add, not the whole method
  public void release(float[] floats) {
    release(floats, false);
  }
  public void release(float[] floats, boolean isAlreadyZeroed) {
    if (currentArrays >= maxArrays || floats.length < neededLength) {
      return;
    }
    if (!isAlreadyZeroed) {
      Arrays.fill(floats, 0.0f);
    }
    syncRelease(floats);
  }
  private synchronized void syncRelease(float[] floats) {
    if (currentArrays >= maxArrays || floats.length < neededLength) {
      return;
    }
    this.floats.add(floats);
    currentArrays++;
  } 

  public void release(double[] doubles) {
    release(doubles, false);
  }
  public void release(double[] doubles, boolean isAlreadyZeroed) {
    if (currentArrays >= maxArrays || doubles.length < neededLength) {
      return;
    }
    if (!isAlreadyZeroed) {
      Arrays.fill(doubles, 0.0d);
    }
    syncRelease(doubles);
  }
  private synchronized void syncRelease(double[] doubles) {
    if (currentArrays >= maxArrays || doubles.length < neededLength) {
      return;
    }
    this.doubles.add(doubles);
    currentArrays++;
  }

  public void release(int[] ints) {
    release(ints, false);
  }
  public void release(int[] ints, boolean isAlreadyZeroed) {
    if (currentArrays >= maxArrays || ints.length < neededLength) {
      return;
    }
    if (!isAlreadyZeroed) {
      Arrays.fill(ints, 0);
    }
    syncRelease(ints);
  }
  private synchronized void syncRelease(int[] ints) {
    if (currentArrays >= maxArrays || ints.length < neededLength) {
      return;
    }
    this.ints.add(ints);
    currentArrays++;
  }

  public void release(long[] longs) {
    release(longs, false);
  }
  public void release(long[] longs, boolean isAlreadyZeroed) {
    if (currentArrays >= maxArrays || longs.length < neededLength) {
      return;
    }
    if (!isAlreadyZeroed) {
      Arrays.fill(longs, 0);
    }
    syncRelease(longs);
  }
  private synchronized void syncRelease(long[] longs) {
    if (currentArrays >= maxArrays || longs.length < neededLength) {
      return;
    }
    this.longs.add(longs);
    currentArrays++;
  }
  
  /**
   * Setting neededLength iterates all arrays and discards those with length less than neededLength.
   * @param neededLength the currently length needed when requesting an array.
   */
  public synchronized void setNeededLength(int neededLength) {
    if (this.neededLength > neededLength) { // Need to verify lengths

      for (int i = floats.size()-1 ; i >= 0 ; i--) {
        if (floats.get(i).length < neededLength) {
          floats.remove(i);
          currentArrays--;
        }
      }
      for (int i = doubles.size()-1 ; i >= 0 ; i--) {
        if (doubles.get(i).length < neededLength) {
          doubles.remove(i);
          currentArrays--;
        }
      }
      for (int i = ints.size()-1 ; i >= 0 ; i--) {
        if (ints.get(i).length < neededLength) {
          ints.remove(i);
          currentArrays--;
        }
      }
      for (int i = longs.size()-1 ; i >= 0 ; i--) {
        if (longs.get(i).length < neededLength) {
          longs.remove(i);
          currentArrays--;
        }
      }
    }
    this.neededLength = neededLength;
  }
  
}

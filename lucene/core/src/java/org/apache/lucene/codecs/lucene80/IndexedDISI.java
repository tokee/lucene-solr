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
package org.apache.lucene.codecs.lucene80;

import java.io.DataInput;
import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * Disk-based implementation of a {@link DocIdSetIterator} which can return
 * the index of the current document, i.e. the ordinal of the current document
 * among the list of documents that this iterator can return. This is useful
 * to implement sparse doc values by only having to encode values for documents
 * that actually have a value.
 * <p>Implementation-wise, this {@link DocIdSetIterator} is inspired of
 * {@link RoaringDocIdSet roaring bitmaps} and encodes ranges of {@code 65536}
 * documents independently and picks between 3 encodings depending on the
 * density of the range:<ul>
 *   <li>{@code ALL} if the range contains 65536 documents exactly,
 *   <li>{@code DENSE} if the range contains 4096 documents or more; in that
 *       case documents are stored in a bit set,
 *   <li>{@code SPARSE} otherwise, and the lower 16 bits of the doc IDs are
 *       stored in a {@link DataInput#readShort() short}.
 * </ul>
 * <p>Only ranges that contain at least one value are encoded.
 * <p>This implementation uses 6 bytes per document in the worst-case, which happens
 * in the case that all ranges contain exactly one document.
 *
 * 
 * To avoid O(n) lookup time complexity, with n being the number of documents, two lookup
 * tables are used:  * A lookup table for block blockCache and index, and a rank structure
 * for DENSE block lookups.
 *
 * The lookup table is an array of {@code long}s with an entry for each block. It allows for
 * direct jumping to the block, as opposed to iteration from the current position and forward
 * one block at a time.
 *
 * Each long entry consists of 2 logical parts:
 *
 * The first 31 bits holds the index (number of set bits in the blocks) up to just before the
 * wanted block. The next 33 bits holds the offset into the underlying slice.
 * As there is a maximum of 2^16 blocks, it follows that the maximum size of any block must
 * not exceed 2^17 bits to avoid overflow. This is currently the case, with the largest
 * block being DENSE and using 2^16 + 32 bits, and is likely to continue to hold as using
 * more than double the amount of bits is unlikely to be an efficient representation.
 * The cache overhead is numDocs/1024 bytes.
 *
 * Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits).
 * In the case of non-existing blocks, the entry in the lookup table has index equal to the
 * previous entry and offset equal to the next non-empty block.
 *
 * The block lookup table is stored at the end of the total block structure.
 *
 *
 * The rank structure for DENSE blocks is an array of unsigned {@code short}s with an entry
 * for each sub-block of 512 bits out of the 65536 bits in the outer DENSE block.
 *
 * Each rank-entry states the number of set bits within the block up to the bit before the
 * bit positioned at the start of the sub-block.
 * Note that that the rank entry of the first sub-block is always 0 and that the last entry can
 * at most be 65536-512 = 65024 and thus will always fit into an unsigned short.
 *
 * The rank structure for a given DENSE block is stored at the beginning of the DENSE block.
 * This ensures locality and keeps logistics simple.
 *
 * @lucene.internal
 */
final class IndexedDISI extends DocIdSetIterator {

  // jump-table time/space trade-offs to consider:
  // The block offsets and the block indexes could be stored in more compressed form with
  // two PackedInts or two MonotonicDirectReaders.
  // The DENSE ranks (128 shorts = 256 bytes) could likewise be compressed. As there is at least
  // 4096 set bits in DENSE blocks, there will be at least one rank with 2^12 bits, so it is
  // doubtful if there is much to gain here.
  
  private static final int BLOCK_SIZE = 65536;   // The number of docIDs that a single block represents
  static final int BLOCK_BITS = 16;
  private static final long BLOCK_INDEX_SHIFT = 33; // Number of bits to shift a lookup entry to get the index
  private static final long BLOCK_INDEX_MASK = ~0L << BLOCK_INDEX_SHIFT; // The index bits in a lookup entry
  private static final long BLOCK_LOOKUP_MASK = ~BLOCK_INDEX_MASK; // The offset bits in a lookup entry

  private static final int RANK_BLOCK_SIZE = 512; // The number of docIDs/bits in each rank-sub-block within a DENSE block
  private static final int RANK_BLOCK_LONGS = 512/Long.SIZE; // The number of longs making up a rank-block (8)
  private static final int RANK_BLOCK_BITS = 9;
  private static final int RANKS_PER_BLOCK = BLOCK_SIZE / RANK_BLOCK_SIZE;

  static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;

  private final long jumpTableOffset; // If -1, the use of jump-table is disabled
  private final long jumpTableEntryCount;

  private static void flush(int block, FixedBitSet buffer, int cardinality, IndexOutput out) throws IOException {
    assert block >= 0 && block < 65536;
    out.writeShort((short) block);
    assert cardinality > 0 && cardinality <= 65536;
    out.writeShort((short) (cardinality - 1));
    if (cardinality > MAX_ARRAY_LENGTH) {
      if (cardinality != 65536) { // all docs are set
        final short[] rank = createRank(buffer);
        for (int i = 0 ; i < rank.length ; i++) {
          out.writeShort(rank[i]);
        }
        for (long word : buffer.getBits()) {
          out.writeLong(word);
        }
      }
    } else {
      BitSetIterator it = new BitSetIterator(buffer, cardinality);
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        out.writeShort((short) doc);
      }
    }
  }

  // Creates a rank-entry (the number of set bits up to a given point) for the buffer.
  // One rank-entry for every 512 bits/8 longs for a total of 128 chars.
  private static short[] createRank(FixedBitSet buffer) {
    final short[] rank = new short[128];
    final long[] bits = buffer.getBits();
    int bitCount = 0;
    for (int word = 0 ; word < 1024 ; word++) {
      if (word >> 3 << 3 == word) { // Every 8 longs
//        System.out.println("Writing rank[" + (word>>3) + "] for docID " + (word*8*Long.BYTES) + ": " + bitCount);
        rank[word >> 3] = (short)bitCount;
      }
      bitCount += Long.bitCount(bits[word]);
    }
    return rank;
  }

  /**
   * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
   * increasing gap-less order.
   * @param it  the document IDs.
   * @param out destination for the blocks.
   * @throws IOException if there was an error writing to out.
   */
  static void writeBitSet(DocIdSetIterator it, IndexOutput out) throws IOException {
    int totalCardinality = 0;
    int blockCardinality = 0;
    final FixedBitSet buffer = new FixedBitSet(1<<16);
    long[] jumps = new long[ArrayUtil.oversize(100, Long.BYTES)];
    jumps[0] = out.getFilePointer(); // First block starts at index 0
    int prevBlock = -1;
    int jumpBlockIndex = 0;

    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      final int block = doc >>> 16;
      if (prevBlock != -1 && block != prevBlock) {
        // Track offset+index from previous block up to current
        jumps = addJumps(jumps, out.getFilePointer(), totalCardinality, jumpBlockIndex, prevBlock+1);
        jumpBlockIndex = prevBlock+1;
        // Flush block
//        System.out.println("Flush with cardinality: " + blockCardinality + " for prevBlock " + prevBlock + " with current=" + block );
        flush(prevBlock, buffer, blockCardinality, out);
        // Reset for next block
        buffer.clear(0, buffer.length());
        totalCardinality += blockCardinality;
        blockCardinality = 0;
      }
      buffer.set(doc & 0xFFFF);
//      System.out.println("B" + block + "." + (doc & 0xFFFF));
      blockCardinality++;
      prevBlock = block;
    }
    if (blockCardinality > 0) {
      jumps = addJumps(jumps, out.getFilePointer(), totalCardinality, jumpBlockIndex, prevBlock+1);
      flush(prevBlock, buffer, blockCardinality, out);
      buffer.clear(0, buffer.length());
      prevBlock++;
    }
    final int lastBlock = prevBlock == -1 ? 0 : prevBlock;
//    jumps = addJumps(jumps, out.getFilePointer(), totalCardinality, lastBlock, lastBlock+1);
    // NO_MORE_DOCS is stored explicitly
    buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
    flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, out);
    // offset+index jump-table stored at the end
    flushBlockJumps(jumps, lastBlock, out);
  }

  private static long[] addJumps(long[] jumps, long offset, long index, int startBlock, int endBlock) {
    jumps = ArrayUtil.grow(jumps, endBlock +1);
    final long jump = (index << BLOCK_INDEX_SHIFT) | offset;
    for (int b = startBlock; b < endBlock; b++) {
//      System.out.println("Adding jump " + b + ": offset=" + offset + " index=" + index + " group " + startBlock + "->" + endBlock);
      jumps[b] = jump;
    }
    return jumps;
  }

  private static void flushBlockJumps(long[] jumps, int blockCount, IndexOutput out) throws IOException {
    for (int i = 0 ; i < blockCount ; i++) {
      final long offset = jumps[i] & BLOCK_LOOKUP_MASK;
      final long index = jumps[i] >>> BLOCK_INDEX_SHIFT;
//      System.out.println("> block[" + i + "]: offset=" + offset + ", index=" + index);
      out.writeLong(jumps[i]);
    }
    // The jumpTableOffset is written at the end along with the number of jump-tabled blocks.
    // This is the last data for the IndexedDISI structure and allows for a single lookup to get jumpTableOffset
    // If the blockCount is 0, the offset is set to -1, disabling the cache
//    if (blockCount == 0) {
//      System.out.println("Disabling cache as blockCount=0");
//    }
    out.writeLong(blockCount == 0 ? -1 : out.getFilePointer()-Long.BYTES*blockCount);
    out.writeInt(blockCount);
  }

  /** The slice that stores the {@link DocIdSetIterator}. */
  private final IndexInput slice;
  private final long cost;

  /**
   * Constructor with jumpTableOffset. All intermediate block from current block to destination block
   * will be visited when skipping forward. Consider using {@link #IndexedDISI(IndexInput, long, long, long, long)}.
   * @param in backing data.
   * @param offset starting offset for blocks in backing data.
   * @param length the number of bytes in the backing data.
   * @param cost normally the number of logical docIDs.
   */
  IndexedDISI(IndexInput in, long offset, long length, long cost) throws IOException {
    this(in.slice("docs", offset, length), cost);
  }

  /**
   * Constructor with jumpTableOffset. Highly recommended for segments with 131K+ documents.
   * @param in backing data.
   * @param offset starting offset for blocks in backing data.
   * @param length the number of bytes in the backing data.
   * @param cost normally the number of logical docIDs.
   * @param jumpTableOffset the offset in backing data for the offset+index jump-table. -1 means no jump-table.
   */
  IndexedDISI(IndexInput in, long offset, long length, long cost, long jumpTableOffset) throws IOException {
    this(in.slice("docs", offset, length), cost, jumpTableOffset);
  }

  /**
   * Constructor without jumpTableOffset. All intermediate block from current block to destination block
   * will be visited when skipping forward. Consider using {@link #IndexedDISI(IndexInput, long, long)}.
   *
   * This constructor allows to pass the slice directly in case it helps reuse.
   * see eg. Lucene70 norms producer's merge instance.
   * @param slice backing data.
   * @param cost normally the number of logical docIDs.
   */
  IndexedDISI(IndexInput slice, long cost) throws IOException {
    this(slice, cost, -1);
  }

  /**
   * Constructor with jumpTableOffset. Highly recommended for segments with 131K+ documents.
   * @param slice backing data.
   * @param cost normally the number of logical docIDs.
   * @param jumpTableOffset the offset in slice for the offset+index jump-table. -1 means no jump-table.
   */
  IndexedDISI(IndexInput slice, long cost, long jumpTableOffset) throws IOException {
    this.slice = slice;
    this.cost = cost;
    final long startOffset = slice.getFilePointer();
    slice.seek(startOffset+slice.length()-Long.BYTES-Integer.BYTES);
    this.jumpTableOffset = slice.readLong();
    this.jumpTableEntryCount = slice.readInt();
    slice.seek(startOffset);
  }

  private int block = -1;
  private long blockEnd;
  private long rankOrigoOffset = -1; // Only used for DENSE blocks
  private int nextBlockIndex = -1;
  Method method;

  private int doc = -1;
  private int index = -1;

  // SPARSE variables
  boolean exists;

  // DENSE variables
  private long word;
  private int wordIndex = -1;
  // number of one bits encountered so far, including those of `word`
  private int numberOfOnes;
  // Used with rank for jumps inside of DENSE
  private int denseOrigoIndex;

  // ALL variables
  private int gap;

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;
    if (block < targetBlock) {
      advanceBlock(targetBlock);
    }
    if (block == targetBlock) {
      if (method.advanceWithinBlock(this, target)) {
        return doc;
      }
      readBlockHeader();
    }
    boolean found = method.advanceWithinBlock(this, block);
    assert found;
    return doc;
  }

  public boolean advanceExact(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;
    if (block < targetBlock) {
      advanceBlock(targetBlock);
    }
    boolean found = block == targetBlock && method.advanceExactWithinBlock(this, target);
    this.doc = target;
    return found;
  }

  private void advanceBlock(int targetBlock) throws IOException {
    final int blockIndex = targetBlock >> 16;
    // If the destination block is 2 blocks or more ahead, we use the jump-table.
    if (jumpTableOffset != -1L && blockIndex >= (block >> 16)+2 && blockIndex < jumpTableEntryCount) {
      slice.seek(jumpTableOffset + Long.BYTES * blockIndex);
      final long jumpEntry = slice.readLong();

      final long offset = jumpEntry & BLOCK_LOOKUP_MASK;
      final long index = jumpEntry >>> BLOCK_INDEX_SHIFT;
      //System.out.println("Jump to block " + blockIndex + " with current block=" + (block >> 16) + ": offset=" + offset + ", index=" + index);
      this.nextBlockIndex = (int) (index - 1); // -1 to compensate for the always-added 1 in readBlockHeader
      slice.seek(offset);
      readBlockHeader();
      return;
    }

    // Fallback to iteration of blocks
    do {
      slice.seek(blockEnd);
      readBlockHeader();
    } while (block < targetBlock);
  }

  private void readBlockHeader() throws IOException {
    block = Short.toUnsignedInt(slice.readShort()) << 16;
    assert block >= 0;
    final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
    index = nextBlockIndex;
    nextBlockIndex = index + numValues;
    if (numValues <= MAX_ARRAY_LENGTH) {
      method = Method.SPARSE;
      blockEnd = slice.getFilePointer() + (numValues << 1);
    } else if (numValues == 65536) {
      method = Method.ALL;
      blockEnd = slice.getFilePointer();
      gap = block - index - 1;
    } else {
      method = Method.DENSE;
      blockEnd = slice.getFilePointer() + RANKS_PER_BLOCK*Short.BYTES + (1 << 13);
      rankOrigoOffset = slice.getFilePointer();
      // Performance consideration: All rank (128 shorts) could be loaded up front, but that would be wasted if the
      // DENSE block is iterated in steps less than 512 bits. As the whole point of rank in DENSE is to speed up
      // large jumps, it seems counter-intuitive to have a non-trivial up-front cost as chances are only few of the
      // rank entries will be used.
      slice.seek(rankOrigoOffset + RANKS_PER_BLOCK*Short.BYTES); // Position at DENSE block bitmap start
      wordIndex = -1;
      numberOfOnes = index + 1;
      denseOrigoIndex = numberOfOnes;
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  public int index() {
    return index;
  }

  @Override
  public long cost() {
    return cost;
  }

  enum Method {
    SPARSE {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        for (; disi.index < disi.nextBlockIndex;) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            disi.doc = disi.block | doc;
            disi.exists = true;
            return true;
          }
        }
        return false;
      }
      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        if (target == disi.doc) {
          return disi.exists;
        }
        for (; disi.index < disi.nextBlockIndex;) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            if (doc != targetInBlock) {
              disi.index--;
              disi.slice.seek(disi.slice.getFilePointer() - Short.BYTES);
              break;
            }
            disi.exists = true;
            return true;
          }
        }
        disi.exists = false;
        return false;
      }
    },
    DENSE {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        final int targetWordIndex = targetInBlock >>> 6;

        // If possible, skip ahead using the rank cache
        disi.rankSkip(disi, target);

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
          disi.word = disi.slice.readLong();
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        if (leftBits != 0L) {
          disi.doc = target + Long.numberOfTrailingZeros(leftBits);
          disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
          return true;
        }

        // There were no set bits at the wanted position. Move forward until one is reached
        while (++disi.wordIndex < 1024) {
          // This could use the rank cache to skip empty spaces >= 512 bits, but it seems unrealistic
          // that such blocks would be DENSE
          disi.word = disi.slice.readLong();
          if (disi.word != 0) {
            disi.index = disi.numberOfOnes;
            disi.numberOfOnes += Long.bitCount(disi.word);
            disi.doc = disi.block | (disi.wordIndex << 6) | Long.numberOfTrailingZeros(disi.word);
            return true;
          }
        }
        // No set bits in the block at or after the wanted position.
        return false;
      }

      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        final int targetWordIndex = targetInBlock >>> 6;

        // If possible, skip ahead using the rank cache
        disi.rankSkip(disi, target);

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
          disi.word = disi.slice.readLong();
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
        return (leftBits & 1L) != 0;
      }


    },
    ALL {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        disi.doc = target;
        disi.index = target - disi.gap;
        return true;
      }
      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        disi.index = target - disi.gap;
        return true;
      }
    };

    /** Advance to the first doc from the block that is equal to or greater than {@code target}.
     *  Return true if there is such a doc and false otherwise. */
    abstract boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException;

    /** Advance the iterator exactly to the position corresponding to the given {@code target}
     * and return whether this document exists. */
    abstract boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException;
  }

  /**
   * If the distance between the current position and the target is > 8 words, the rank cache will
   * be used to guarantee a worst-case of 1 rank-lookup and 7 word-read-and-count-bits operations.
   * Note: This does not guarantee a skip up to target, only up to nearest rank boundary. It is the
   * responsibility of the caller to iterate further to reach target.
   * @param disi standard DISI.
   * @param target the wanted docID for which to calculate set-flag and index.
   * @throws IOException if a DISI seek failed.
   */
  private static void rankSkip(IndexedDISI disi, int target) throws IOException {
    final int targetInBlock = target & 0xFFFF;       // Lower 16 bits
    final int targetWordIndex = targetInBlock >>> 6; // long: 2^6 = 64

    // If the distance between the current position and the target is < 8 longs
    // there is no sense in using rank
    if (targetWordIndex - disi.wordIndex < RANK_BLOCK_LONGS) {
      return;
    }

    // Resolve the rank as close to targetInBlock as possible (maximum distance is 8 longs)
    final int rankIndex = targetInBlock >> RANK_BLOCK_BITS; // 8 longs: 2^3 * 2^6 = 512
    disi.slice.seek(disi.rankOrigoOffset + rankIndex*Short.BYTES);
    final int rank = disi.denseOrigoIndex + (disi.slice.readShort() & 0xFFFF);
//    System.out.println("Read rank[" + rankIndex + "] as " + (rank-disi.denseOrigoIndex) + " for block docID " + targetInBlock);

    // Position the counting logic just after the rank point
    final int rankAlignedWordIndex = rankIndex << RANK_BLOCK_BITS >> 6;
    disi.slice.seek(disi.rankOrigoOffset + RANKS_PER_BLOCK*Short.BYTES + rankAlignedWordIndex*Long.BYTES);
    long rankWord = disi.slice.readLong();
    int rankNOO = rank + Long.bitCount(rankWord);

    disi.wordIndex = rankAlignedWordIndex;
    disi.word = rankWord;
    disi.numberOfOnes = rankNOO;
  }
}

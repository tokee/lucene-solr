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

package org.apache.solr.handler.export;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesIterator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.common.MapWriter;

/**
 * Handles the logistics of a {@link org.apache.lucene.index.DocValuesIterator} and previously requested readers
 * and docIDs, minimizing re-creation.
 */
abstract class FieldWriterImpl<DVI extends DocValuesIterator> extends FieldWriter {
  protected final String field;
  protected DVI docValuesIterator = null;
  private LeafReader currentReader = null;
  protected boolean useSortValueIfPossible = true;

  public FieldWriterImpl(String field) {
    this.field = field;
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, MapWriter.EntryWriter out, int fieldIndex) throws IOException {

    // Reuse the value from the SortDoc if available
    if (useSortValueIfPossible) {
      SortValue sortValue = sortDoc.getSortValue(this.field);
      if (sortValue != null) {
        if (!sortValue.isPresent()) {
          return false;
        }
        Object val = sortValue.getCurrentValue();
        out.put(field, externalize(val));
        return true;
      }
    }

    checkIterator(sortDoc, reader);

    // Advance to value
    if (!docValuesIterator.advanceExact(sortDoc.docId)) {
      return false;
    }

    // Fetch value and add it to the collector
    addCurrentValue(out);
    return true;
  }

  // Create a new DocValuesIterator only if needed
  private void checkIterator(SortDoc sortDoc, LeafReader reader) throws IOException {
    if (docValuesIterator == null || currentReader == null || !currentReader.equals(reader) ||
        sortDoc.docId < docValuesIterator.docID()) {
//      if (docValuesIterator != null  && sortDoc.docId != 0) { // Why the secondary check? Are docIDs relative to segment?
//        System.out.println("Unsorted for docID " + sortDoc.docId + " with current docID " + docValuesIterator.docID() + " for field " + this.field +
//            " currentReader " + currentReader + " vs " + reader + ", dvi==null " + (docValuesIterator==null) + " for " + this);
//      }
      docValuesIterator = createDocValuesIterator(reader, this.field);
      currentReader = reader;
    }
  }

  /**
   * @return external representation of the value. Defaults to identity.
   */
  protected Object externalize(Object val) {
    return val;
  }

  /**
   * Add the current value in the docValuesIterator to out in externalized format for {@link #field}.
   * Sample: {@code out.put(field, docValuesIterator.longValue);
   */
  protected abstract void addCurrentValue(MapWriter.EntryWriter out) throws IOException;

  /**
   * @return a typed DocValuesIterater for the field for the reader.
   */
  protected abstract DVI createDocValuesIterator(LeafReader reader, String field) throws IOException;

}
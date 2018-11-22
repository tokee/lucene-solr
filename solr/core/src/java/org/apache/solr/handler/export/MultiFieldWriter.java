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
import java.util.Date;
import java.util.function.LongFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesIterator;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

class MultiFieldWriter extends FieldWriterImpl<DocValuesIterator> {
  private FieldType fieldType;
  private SchemaField schemaField;
  private boolean numeric;
  private CharsRefBuilder cref = new CharsRefBuilder();
  private final LongFunction<Object> bitsToValue;

  private SortedNumericDocValues numDVI = null;
  private SortedSetDocValues setDVI = null;

  public MultiFieldWriter(String field, FieldType fieldType, SchemaField schemaField, boolean numeric) {
    super(field);
    this.fieldType = fieldType;
    this.schemaField = schemaField;
    this.numeric = numeric;
    if (this.fieldType.isPointField()) {
      bitsToValue = bitsToValue(fieldType);
    } else {
      bitsToValue = null;
    }
    useSortValueIfPossible = false;
  }

  @Override
  protected void addCurrentValue(MapWriter.EntryWriter out) throws IOException {
    if (this.fieldType.isPointField()) {
      out.put(this.field,
          (IteratorWriter) w -> {
            for (int i = 0; i < numDVI.docValueCount(); i++) {
              w.add(bitsToValue.apply(numDVI.nextValue()));
            }
          });
      return;
    }
    
    out.put(this.field,
        (IteratorWriter) w -> {
          long o;
          while((o = setDVI.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            BytesRef ref = setDVI.lookupOrd(o);
            fieldType.indexedToReadable(ref, cref);
            IndexableField f = fieldType.createField(schemaField, cref.toString());
            if (f == null) w.add(cref.toString());
            else w.add(fieldType.toObject(f));
          }
        });
  }

  @Override
  protected DocValuesIterator createDocValuesIterator(LeafReader reader, String field) throws IOException {
    return fieldType.isPointField() ?
        (numDVI = DocValues.getSortedNumeric(reader, this.field)) :
        (setDVI = DocValues.getSortedSet(reader, this.field));
  }

  static LongFunction<Object> bitsToValue(FieldType fieldType) {
    switch (fieldType.getNumberType()) {
      case LONG:
        return (bits)-> bits;
      case DATE:
        return (bits)-> new Date(bits);
      case INTEGER:
        return (bits)-> (int)bits;
      case FLOAT:
        return (bits)-> NumericUtils.sortableIntToFloat((int)bits);
      case DOUBLE:
        return (bits)-> NumericUtils.sortableLongToDouble(bits);
      default:
        throw new AssertionError("Unsupported NumberType: " + fieldType.getNumberType());
    }
  }
}

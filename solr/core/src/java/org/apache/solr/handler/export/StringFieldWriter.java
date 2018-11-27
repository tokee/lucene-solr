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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.MapWriter;
import org.apache.solr.schema.FieldType;

class StringFieldWriter extends FieldWriterImpl<SortedDocValues> {
  private FieldType fieldType;
  private CharsRefBuilder cref = new CharsRefBuilder();

  public StringFieldWriter(String field, FieldType fieldType) {
    super(field);
    this.fieldType = fieldType;
  }

  @Override
  protected void addCurrentValue(MapWriter.EntryWriter out) throws IOException {
    int ord = docValuesIterator.ordValue();
    BytesRef ref = docValuesIterator.lookupOrd(ord);
    out.put(this.field, externalize(ref));
  }

  @Override
  protected SortedDocValues createDocValuesIterator(LeafReader reader, String field) throws IOException {
    return DocValues.getSorted(reader, this.field);
  }

  @Override
  protected Object externalize(Object val) {
    fieldType.indexedToReadable((BytesRef) val, cref);
    return cref.toString();
  }
}
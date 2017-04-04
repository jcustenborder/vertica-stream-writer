/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.vertica;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class VerticaStreamWriterBuilder {
  String schema;
  String database;
  String table;
  int rowBufferSize = 1024 * 1024;

  public int rowBufferSize() {
    return rowBufferSize;
  }

  public VerticaStreamWriterBuilder rowBufferSize(int rowBufferSize) {
    this.rowBufferSize = rowBufferSize;
    return this;
  }

  VerticaStreamWriterType streamWriterType = VerticaStreamWriterType.BINARY;
  VerticaCompressionType compressionType = VerticaCompressionType.NONE;
  List<VerticaColumnInfo> columnInfos = new ArrayList<>();

  public VerticaStreamWriter build(OutputStream outputStream) throws IOException {
    VerticaStreamWriter writer;

    switch (this.streamWriterType) {
      case BINARY:
        writer = new VerticaBinaryStreamWriter(this, outputStream);
        break;
      default:
        throw new UnsupportedEncodingException(
            String.format("Unsupported stream writer type of %s", this.streamWriterType)
        );
    }


    return writer;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaType type, int size) {
    VerticaColumnInfo columnInfo = new VerticaColumnInfo(name, type, size);
    this.columnInfos.add(columnInfo);
    return this;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaType type) {
    VerticaColumnInfo columnInfo = new VerticaColumnInfo(name, type);
    this.columnInfos.add(columnInfo);
    return this;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaType type, int precision, int scale) {
    VerticaColumnInfo columnInfo = new VerticaColumnInfo(name, type, -1, precision, scale);
    this.columnInfos.add(columnInfo);
    return this;
  }

  public String schema() {
    return schema;
  }

  public VerticaStreamWriterBuilder schema(String schema) {
    this.schema = schema;
    return this;
  }

  public String database() {
    return database;
  }

  public VerticaStreamWriterBuilder database(String database) {
    this.database = database;
    return this;
  }

  public String table() {
    return table;
  }

  public VerticaStreamWriterType streamWriterType() {
    return streamWriterType;
  }

  public VerticaStreamWriterBuilder streamWriterType(VerticaStreamWriterType streamWriterType) {
    this.streamWriterType = streamWriterType;
    return this;
  }

  public VerticaCompressionType compressionType() {
    return compressionType;
  }

  public VerticaStreamWriterBuilder compressionType(VerticaCompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  public VerticaStreamWriterBuilder table(String table) {
    this.table = table;
    return this;
  }
}

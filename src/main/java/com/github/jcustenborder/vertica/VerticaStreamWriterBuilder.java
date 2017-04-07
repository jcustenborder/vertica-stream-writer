/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.vertica;

import com.google.common.base.Preconditions;
import org.anarres.lzo.LzoOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;

public class VerticaStreamWriterBuilder {
  private static final Logger log = LoggerFactory.getLogger(VerticaStreamWriterBuilder.class);
  String schema;
  String table;
  int rowBufferSize = 1024 * 1024;
  Integer compressionLevel;

  public Integer compressionLevel() {
    return compressionLevel;
  }

  public VerticaStreamWriterBuilder compressionLevel(Integer compressionLevel) {
    this.compressionLevel = compressionLevel;
    return this;
  }

  public int rowBufferSize() {
    return rowBufferSize;
  }

  static final int MIN_ROW_BUFFER = 100;

  public VerticaStreamWriterBuilder rowBufferSize(int rowBufferSize) {
    Preconditions.checkState(
        rowBufferSize > MIN_ROW_BUFFER,
        "rowBufferSize must be greater than %s bytes.",
        MIN_ROW_BUFFER);
    this.rowBufferSize = rowBufferSize;
    return this;
  }

  VerticaStreamWriterType streamWriterType = VerticaStreamWriterType.NATIVE;
  VerticaCompressionType compressionType = VerticaCompressionType.UNCOMPRESSED;
  List<VerticaColumnInfo> columnInfos = new ArrayList<>();

  public VerticaStreamWriter build(OutputStream outputStream) throws IOException {
    Preconditions.checkNotNull(outputStream, "outputStream cannot be null.");
    Preconditions.checkNotNull(this.table, "table cannot be null or empty.");
    Preconditions.checkState(!this.table.isEmpty(), "table cannot be null or empty.");


    final OutputStream stream;

    switch (this.compressionType) {
      case BZIP:
        if (null != this.compressionLevel) {
          Preconditions.checkState(
              this.compressionLevel >= BZip2CompressorOutputStream.MIN_BLOCKSIZE &&
                  this.compressionLevel <= BZip2CompressorOutputStream.MAX_BLOCKSIZE,
              "compressionLevel must be >= %s and <= %s. %s is invalid.",
              BZip2CompressorOutputStream.MIN_BLOCKSIZE,
              BZip2CompressorOutputStream.MAX_BLOCKSIZE,
              this.compressionLevel
          );
          log.debug("Creating BZip2CompressorOutputStream with compressionLevel {}.", this.compressionLevel);
          stream = new BZip2CompressorOutputStream(outputStream, this.compressionLevel);
        } else {
          log.debug("Creating BZip2CompressorOutputStream with default compressionLevel.");
          stream = new BZip2CompressorOutputStream(outputStream);
        }
        break;
      case GZIP:
        if (null != this.compressionLevel) {
          Preconditions.checkState(
              this.compressionLevel >= Deflater.NO_COMPRESSION &&
                  this.compressionLevel <= Deflater.BEST_COMPRESSION,
              "compressionLevel must be >= %s and <= %s. %s is invalid.",
              Deflater.NO_COMPRESSION,
              Deflater.BEST_COMPRESSION,
              this.compressionLevel
          );
          GzipParameters parameters = new GzipParameters();
          parameters.setCompressionLevel(this.compressionLevel);
          log.debug("Creating GzipCompressorOutputStream with compressionLevel {}.", this.compressionLevel);
          stream = new GzipCompressorOutputStream(outputStream, parameters);
        } else {
          log.debug("Creating GzipCompressorOutputStream with default compressionLevel.");
          stream = new GzipCompressorOutputStream(outputStream);
        }
        break;
      case UNCOMPRESSED:
        stream = outputStream;
        break;
      case LZO:
        log.debug("Creating LzoOutputStream with default compressionLevel.");
        stream = new LzoOutputStream(outputStream);
        break;
      default:
        throw new UnsupportedEncodingException(
            String.format("Unsupported compression type of %s", this.streamWriterType)
        );
    }

    VerticaStreamWriter writer;

    switch (this.streamWriterType) {
      case NATIVE:
        writer = new VerticaNativeStreamWriter(this, stream);
        break;
      default:
        throw new UnsupportedEncodingException(
            String.format("Unsupported stream writer type of %s", this.streamWriterType)
        );
    }

    return writer;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaColumnType type, int size) {
    VerticaColumnInfo columnInfo = new VerticaColumnInfo(name, type, size);
    this.columnInfos.add(columnInfo);
    return this;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaColumnType type) {
    VerticaColumnInfo columnInfo = new VerticaColumnInfo(name, type);
    this.columnInfos.add(columnInfo);
    return this;
  }

  public VerticaStreamWriterBuilder column(String name, VerticaColumnType type, int precision, int scale) {
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

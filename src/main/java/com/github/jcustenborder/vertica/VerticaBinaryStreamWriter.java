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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

class VerticaBinaryStreamWriter implements VerticaStreamWriter {
  private static final Logger log = LoggerFactory.getLogger(VerticaBinaryStreamWriter.class);
  final OutputStream outputStream;
  final WritableByteChannel channel;
  final ByteBuffer rowBuffer;
  final ByteBuffer rowHeaderBuffer;
  final List<VerticaColumnInfo> columns;
  final int nullMarkerBufferSize;
  static final byte[] HEADER = BaseEncoding.base16().decode("4E41544956450AFF0D0A00");

  VerticaBinaryStreamWriter(VerticaStreamWriterBuilder builder, OutputStream outputStream) throws IOException {
    this.outputStream = outputStream;
    this.channel = Channels.newChannel(this.outputStream);
    this.columns = ImmutableList.copyOf(builder.columnInfos);
    log.info("ctor() - Allocating row buffer of {} bytes.", builder.rowBufferSize);
    this.rowBuffer = ByteBuffer.allocate(builder.rowBufferSize);
    this.rowBuffer.order(ByteOrder.LITTLE_ENDIAN);

    this.nullMarkerBufferSize = (int) (Math.ceil(this.columns.size() / 8D));

    final int rowHeaderSize = this.nullMarkerBufferSize + 4;
    log.trace("ctor() - Allocating {} byte(s) for row header.", rowHeaderSize);
    this.rowHeaderBuffer = ByteBuffer.allocate(rowHeaderSize).order(ByteOrder.LITTLE_ENDIAN);

    log.trace("ctor() - Writing header");
    this.rowBuffer.put(HEADER);

    final int headerLength = (this.columns.size() * 4) + 5;
    log.trace("ctor() - Header length {} byte(s).", headerLength);
    this.rowBuffer.putInt(headerLength);
    this.rowBuffer.putShort((short) 1);
    this.rowBuffer.put(Constants.ZERO);
    this.rowBuffer.putShort((short) this.columns.size());


    for (VerticaColumnInfo columnInfo : this.columns) {
      log.trace("ctor() - Setting length for '{}' to {} byte(s).", columnInfo.name, columnInfo.size);
      this.rowBuffer.putInt(columnInfo.size);
    }

    this.rowBuffer.flip();
    log.trace("ctor() - Writing {} byte(s) for header.", this.rowBuffer.remaining());
    this.channel.write(this.rowBuffer);
  }

  @Override
  public void close() throws IOException {

  }

  byte[] nullMarkers(Object[] row) {
    final byte[] buffer = new byte[this.nullMarkerBufferSize];

    for (int i = 0; i < row.length; i++) {
      boolean isNull = null == row[i];
      int index = (int) Math.floor((double) i / 8.0d);
      int bitIdx = i - (index * 8);
      if (isNull) {
        buffer[index] |= (1 << bitIdx);
        log.trace("nullMarkers() - Setting bit {} to {}. index={}", i, isNull, index);
      }
    }

    return buffer;
  }

  @Override
  public void write(Object[] row) throws IOException {
    Preconditions.checkNotNull(row, "row cannot be null.");
    Preconditions.checkState(this.columns.size() == row.length, "The length of the row array must be equal to the number of columns");

    this.rowBuffer.clear();
    this.rowHeaderBuffer.clear();

    for (int i = 0; i < row.length; i++) {
      VerticaColumnInfo columnInfo = this.columns.get(i);
      log.trace("write() - Writing value for {} - {}", i, columnInfo.name);
      columnInfo.encode(this.rowBuffer, row[i]);
    }
    log.trace("write() - wrote {} byte(s)", this.rowBuffer.position());
    byte[] nullMarker = nullMarkers(row);
    this.rowBuffer.flip();
    this.rowHeaderBuffer.putInt(this.rowBuffer.remaining());
    this.rowHeaderBuffer.put(nullMarker);
    this.rowHeaderBuffer.flip();
    log.trace("write() - writing {} byte(s) for header.", this.rowHeaderBuffer.remaining());
    this.channel.write(this.rowHeaderBuffer);
    log.trace("write() - writing {} byte(s) for row.", this.rowBuffer.remaining());
    this.channel.write(this.rowBuffer);
  }
}

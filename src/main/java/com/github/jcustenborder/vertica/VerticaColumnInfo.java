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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.TimeZone;

//https://my.vertica.com/docs/8.0.x/HTML/index.htm#Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm

public class VerticaColumnInfo {
  static final long THEIR_EPOCH = 946684800000L;
  static final long THEIR_EPOCH_MICRO = THEIR_EPOCH * 1000L;
  private static final Logger log = LoggerFactory.getLogger(VerticaColumnInfo.class);
  private static final byte TRUE = (byte) 0x01;
  private static final byte FALSE = (byte) 0x00;
  private static final byte ZERO = FALSE;
  final String name;
  final VerticaType type;
  final int size;
  final int precision;
  final int scale;
  final static TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
  final Calendar calendar;


  public VerticaColumnInfo(String name, VerticaType type, int size, int precision, int scale) {
    Preconditions.checkNotNull(name, "name cannot be null.");
    this.name = name;
    this.type = type;
    this.size = size;

    if (VerticaType.NUMERIC == type) {
      Preconditions.checkState(precision > 0, "precision must be greater than zero.");
      Preconditions.checkState(scale > -1, "scale must be greater than -1.");
    }

    this.precision = precision;
    this.scale = scale;
    this.calendar = Calendar.getInstance(UTC_TIMEZONE);
  }

  public VerticaColumnInfo(String name, VerticaType type) {
    this(name, type, sizeForType(type), -1, -1);
  }

  public VerticaColumnInfo(String name, VerticaType type, int size) {
    this(name, type, size, -1, -1);
  }

  final static int sizeForType(VerticaType type) {
    int size;

    switch (type) {
      case BOOLEAN:
        size = 1;
        break;
      case FLOAT:
        size = 8;
        break;
      case DATE:
        size = 8;
        break;
      case TIME:
        size = 8;
        break;
      case TIMETZ:
        size = 8;
        break;
      case TIMESTAMP:
        size = 8;
        break;
      case TIMESTAMPTZ:
        size = 8;
        break;
      case INTERVAL:
        size = 8;
        break;
      case VARBINARY:
      case VARCHAR:
        size = -1;
        break;
      default:
        throw new IllegalStateException(String.format(
            "Size must be specified for type '%s'",
            type
        ));
    }
    return size;
  }

  void writeFloat(ByteBuffer buffer, Object value) {
    log.trace("writeFloat() - value = {}", value);

    Number number = (Number) value;
    buffer.putDouble(number.doubleValue());
  }

  void writeBoolean(ByteBuffer buffer, Object value) {
    log.trace("writeBoolean() - value = {}", value);
    boolean bool = (boolean) value;
    buffer.put(bool ? TRUE : FALSE);
  }

  void writeChar(ByteBuffer buffer, Object value) {
    log.trace("writeChar() - value = {}", value);

    ByteBuffer valueBuffer = Charsets.UTF_8.encode(value.toString());
    Preconditions.checkState(
        this.size >= valueBuffer.remaining(),
        "Encoded value for '%s' is %s byte(s) but the column is only %s byte(s).",
        this.name,
        valueBuffer.remaining(),
        this.size
    );

    buffer.put(valueBuffer);
    int padding = this.size - valueBuffer.capacity();
    log.trace("writeChar() - padding value by {} byte(s).");
    for (int i = 0; i < padding; i++) {
      buffer.put(FALSE);
    }
  }

  void writeBinary(ByteBuffer buffer, Object value) {
    log.trace("writeBinary() - value = {}", value);

    byte[] valueBuffer = (byte[]) value;

    Preconditions.checkState(
        this.size >= valueBuffer.length,
        "Encoded value for '%s' is %s byte(s) but the column is only %s byte(s).",
        this.name,
        valueBuffer.length,
        this.size
    );

    buffer.put(valueBuffer);
    int padding = this.size - valueBuffer.length;
    log.trace("writeBinary() - padding value by {} byte(s).");
    for (int i = 0; i < padding; i++) {
      buffer.put(FALSE);
    }
  }

  void writeVarchar(ByteBuffer buffer, Object value) {
    log.trace("writeVarchar() - value = {}", value);
    ByteBuffer valueBuffer = Charsets.UTF_8.encode(value.toString());
    log.trace("writeVarchar() - writing {} byte(s).", valueBuffer.remaining());
    buffer.putInt(valueBuffer.remaining());
    buffer.put(valueBuffer);
  }

  void writeVarbinary(ByteBuffer buffer, Object value) {
    log.trace("writeVarbinary() - value = {}", value);
    byte[] valueBuffer = (byte[]) value;
    log.trace("writeVarbinary() - writing {} byte(s).", valueBuffer.length);
    buffer.putInt(valueBuffer.length);
    buffer.put(valueBuffer);
  }

  void writeInteger(ByteBuffer buffer, Object value) {
    log.trace("writeInteger() - value = {}", value);

    Number number = (Number) value;

    switch (this.size) {
      case 1:
        buffer.put(number.byteValue());
        break;
      case 2:
        buffer.putShort(number.shortValue());
        break;
      case 4:
        buffer.putInt(number.intValue());
        break;
      case 8:
        buffer.putLong(number.longValue());
        break;
    }
  }

  void writeDate(ByteBuffer buffer, Object value) {
    log.trace("writeDate() - value = {}", value);
    final long input = toDateStorage(value);
    long storage = (input - THEIR_EPOCH) / (1000 * 60 * 60 * 24);
    log.trace("writeDate() - storage = {}", storage);
    buffer.putLong(storage);
  }

  void writeTimestamp(ByteBuffer buffer, Object value) {
    log.trace("writeTimestamp() - value = {}", value);
    final long input = toDateStorage(value);
    long storage = (input * 1000L - THEIR_EPOCH_MICRO);
    log.trace("writeTimestamp() - storage = {}", storage);
    buffer.putLong(storage);
  }

  void writeTimestampTZ(ByteBuffer buffer, Object value) {
    log.trace("writeTimestampTZ() - value = {}", value);
    final long input = toDateStorage(value);
    long storage = (input * 1000L - THEIR_EPOCH_MICRO);
    log.trace("writeTimestampTZ() - storage = {}", storage);
    buffer.putLong(storage);
  }

  void writeTime(ByteBuffer buffer, Object value) {
    log.trace("writeTime() - value = {}", value);
    final long input = toDateStorage(value);
    this.calendar.setTimeInMillis(input);
    this.calendar.set(2000, 0, 01);
    long storage = (this.calendar.getTimeInMillis() * 1000L - THEIR_EPOCH_MICRO);
    log.trace("writeTime() - storage = {}", storage);
    buffer.putLong(storage);
  }


  void writeTimeTZ(ByteBuffer buffer, Object value) {

  }

  void writeNumeric(ByteBuffer buffer, Object value) {
    /*
    This method needs some love. I'm not super familiar with what is going on here but I'm getting a correct value
    based on the document
     */
    log.trace("writeNumeric() - value = {}", value);
    final BigDecimal decimal = (BigDecimal) value;

    Preconditions.checkState(
        this.scale == decimal.scale(),
        "Scale for '%s' is mismatched. Value(%s) does not match definition of %s.",
        decimal.scale(),
        this.scale
    );

    final BigInteger unscaled = decimal.unscaledValue();
    byte[] unscaledBuffer = unscaled.toByteArray();

    double b = Math.ceil(((38D / 19D) + 1D) * 8D);
    log.trace("writeNumeric() - bufferSize:{}", b);
    int bufferSize = (int) Math.ceil(((38D / 19D) + 1D) * 8D);
    log.trace("writeNumeric() - bufferSize:{}", bufferSize);
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    final int bufferMinusScale = bufferSize - 5;
    final int paddingNeeded = bufferMinusScale - unscaledBuffer.length;
    log.trace("writeNumeric() - Padding with {} byte(s).", paddingNeeded);
    for (int i = 0; i < paddingNeeded; i++) {
      byteBuffer.put(ZERO);
    }
    for (int i = unscaledBuffer.length - 1; i >= 0; i--) {
      byteBuffer.put(unscaledBuffer[i]);
    }
    byteBuffer.put(ZERO);
    byteBuffer.putInt(scale);
    byteBuffer.flip();
    buffer.put(byteBuffer);
  }

  private long toDateStorage(Object value) {
    final long input;

    if (value instanceof java.util.Date) {
      input = ((java.util.Date) value).getTime();
    } else if (value instanceof java.sql.Date) {
      input = ((java.sql.Date) value).getTime();
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Type '%s' is not supported.",
              value.getClass().getName()
          )
      );
    }
    return input;
  }

  void writeInterval(ByteBuffer buffer, Object value) {
    Number number = (Number) value;
    buffer.putLong(number.longValue());
  }

  public void encode(ByteBuffer buffer, Object value) {
    Preconditions.checkNotNull(buffer, "buffer cannot be null.");
    Preconditions.checkState(ByteOrder.LITTLE_ENDIAN == buffer.order(), "buffer.order() must be LITTLE_ENDIAN.");
    if (null == value) {
      log.trace("encode() - Skipping due to null value.");
      return;
    }

    buffer.clear();

    switch (this.type) {
      case INTEGER:
        writeInteger(buffer, value);
        break;
      case FLOAT:
        writeFloat(buffer, value);
        break;
      case BOOLEAN:
        writeBoolean(buffer, value);
        break;
      case VARCHAR:
        writeVarchar(buffer, value);
        break;
      case CHAR:
        writeChar(buffer, value);
        break;
      case BINARY:
        writeBinary(buffer, value);
        break;
      case VARBINARY:
        writeVarbinary(buffer, value);
        break;
      case DATE:
        writeDate(buffer, value);
        break;
      case TIMESTAMP:
        writeTimestamp(buffer, value);
        break;
      case TIMESTAMPTZ:
        writeTimestampTZ(buffer, value);
        break;
      case TIME:
        writeTime(buffer, value);
        break;
      case TIMETZ:
        writeTimeTZ(buffer, value);
        break;
      case NUMERIC:
        writeNumeric(buffer, value);
        break;
      case INTERVAL:
        writeInterval(buffer, value);
        break;

    }
  }

}

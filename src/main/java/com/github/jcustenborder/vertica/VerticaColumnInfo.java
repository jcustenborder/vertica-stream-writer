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

import com.github.jcustenborder.vertica.binary.Encoder;
import com.github.jcustenborder.vertica.binary.Encoders;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;

//https://my.vertica.com/docs/8.0.x/HTML/index.htm#Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm

/**
 * Class is used to define a column in a Vertica table.
 */
public class VerticaColumnInfo {
  private static final Logger log = LoggerFactory.getLogger(VerticaColumnInfo.class);
  final String name;
  final VerticaColumnType type;
  final int size;
  final int precision;
  final int scale;
  final Calendar calendar;
  final Encoders encoders = new Encoders();

  /**
   * Name of the column.
   *
   * @return Name of the column.
   */
  public String name() {
    return name;
  }

  /**
   * Type of column.
   *
   * @return Type of column.
   */
  public VerticaColumnType type() {
    return type;
  }

  /**
   * The size of the column.
   *
   * @return The size of the column.
   */
  public int size() {
    return size;
  }

  /**
   * The precision of the column.
   *
   * @return The precision of the column.
   */
  public int precision() {
    return precision;
  }

  /**
   * The scale of the column.
   *
   * @return The scale of the column.
   */
  public int scale() {
    return scale;
  }

  VerticaColumnInfo(String name, VerticaColumnType type, int size, int precision, int scale) {
    Preconditions.checkNotNull(name, "name cannot be null.");
    this.name = name;
    this.type = type;

    if (VerticaColumnType.NUMERIC == type) {
      Preconditions.checkState(precision > 0, "precision must be greater than zero.");
      Preconditions.checkState(scale > -1, "scale must be greater than -1.");
      this.size = numericSize(precision);
    } else {
      this.size = size;
    }

    this.precision = precision;
    this.scale = scale;
    this.calendar = Calendar.getInstance(Constants.UTC_TIMEZONE);
  }

  VerticaColumnInfo(String name, VerticaColumnType type) {
    this(name, type, sizeForType(type), -1, -1);
  }

  VerticaColumnInfo(String name, VerticaColumnType type, int size) {
    this(name, type, size, -1, -1);
  }

  VerticaColumnInfo(String name, VerticaColumnType type, int precision, int scale) {
    this(name, type, numericSize(precision), precision, scale);
  }

  final static int sizeForType(VerticaColumnType type) {
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

  static int numericSize(int precision) {
    return ((precision / 19) + 1) * 8;
  }

  void encode(ByteBuffer buffer, Object value) {
    Preconditions.checkNotNull(buffer, "buffer cannot be null.");
    Preconditions.checkState(ByteOrder.LITTLE_ENDIAN == buffer.order(), "buffer.order() must be LITTLE_ENDIAN.");
    if (null == value) {
      log.trace("encode() - Skipping due to null value.");
      return;
    }

    Encoder encoder = this.encoders.get(this.type, value);
    if (null == encoder) {
      throw new UnsupportedOperationException(
          String.format(
              "Encoder for %s:%s was found",
              this.type,
              value.getClass().getName()
          )
      );
    }
    encoder.encode(buffer, value, this.name, this.size, this.scale);
  }

}

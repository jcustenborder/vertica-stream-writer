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

import com.google.common.base.MoreObjects;
import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class VerticaColumnInfoTest {
  private static final Logger log = LoggerFactory.getLogger(VerticaColumnInfoTest.class);

  @TestFactory
  public Stream<DynamicTest> staticSize() {
    Map<VerticaColumnType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaColumnType.BOOLEAN, 1);
    testcases.put(VerticaColumnType.FLOAT, 8);
    testcases.put(VerticaColumnType.DATE, 8);
    testcases.put(VerticaColumnType.TIME, 8);
    testcases.put(VerticaColumnType.TIMETZ, 8);
    testcases.put(VerticaColumnType.TIMESTAMP, 8);
    testcases.put(VerticaColumnType.TIMESTAMPTZ, 8);
    testcases.put(VerticaColumnType.INTERVAL, 8);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals((int) entry.getValue(), columnInfo.size, "size should match.");
    }));
  }

  @TestFactory
  public Stream<DynamicTest> variableSize() {
    Map<VerticaColumnType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaColumnType.VARBINARY, 1234);
    testcases.put(VerticaColumnType.VARCHAR, 56442);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals(-1, columnInfo.size, "size should match.");
    }));
  }

  @TestFactory
  public Stream<DynamicTest> userDefinedSize() {
    Map<VerticaColumnType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaColumnType.CHAR, 1234);
    testcases.put(VerticaColumnType.BINARY, 56442);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey(), entry.getValue());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals((int) entry.getValue(), columnInfo.size, "size should match.");
    }));
  }

  static class NumericSizeTestCase {
    final int precision;
    final int scale;
    final int size;

    NumericSizeTestCase(int precision, int scale, int size) {
      this.precision = precision;
      this.scale = scale;
      this.size = size;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("precision", this.precision)
          .add("scale", this.scale)
          .add("size", this.size)
          .toString();
    }
  }

  static NumericSizeTestCase decimal(int precision, int scale, int size) {
    return new NumericSizeTestCase(precision, scale, size);
  }

  @TestFactory
  public Stream<DynamicTest> numericSize() {
    return Arrays.asList(
        decimal(38, 0, 24)
    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo("test", VerticaColumnType.NUMERIC, testCase.precision, testCase.scale);
      assertEquals(testCase.size, columnInfo.size, "size does not match");
      assertEquals(testCase.precision, columnInfo.precision, "precision does not match");
      assertEquals(testCase.scale, columnInfo.scale, "scale does not match");
    }));
  }

  static class EncodeTestCase {

    final int size;
    final VerticaColumnType type;
    final Object input;
    final String expectedValue;
    final int precision;
    final int scale;


    EncodeTestCase(int size, VerticaColumnType type, Object input, String expectedValue, int precision, int scale) {
      this.size = size;
      this.type = type;
      this.input = input;
      this.expectedValue = expectedValue;
      this.precision = precision;
      this.scale = scale;
    }

    EncodeTestCase(int size, VerticaColumnType type, Object input, String expectedValue) {
      this(size, type, input, expectedValue, -1, -1);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .omitNullValues()
          .add("type", this.type)
          .add("size", this.size)
          .add("input", this.input)
          .toString();
    }
  }

  static EncodeTestCase of(
      int size,
      VerticaColumnType type,
      Object input,
      String expectedValue) {
    return new EncodeTestCase(size, type, input, expectedValue);
  }

  static EncodeTestCase of(
      int size,
      VerticaColumnType type,
      Object input,
      String expectedValue,
      int precision,
      int scale) {
    return new EncodeTestCase(size, type, input, expectedValue, precision, scale);
  }

  static EncodeTestCase nulls(int size, VerticaColumnType type) {
    return of(size, type, null, null);
  }

  @TestFactory
  public Stream<DynamicTest> encodeNulls() {
    return Arrays.asList(
        nulls(1, VerticaColumnType.INTEGER),
        nulls(2, VerticaColumnType.INTEGER),
        nulls(4, VerticaColumnType.INTEGER),
        nulls(8, VerticaColumnType.INTEGER),
        nulls(8, VerticaColumnType.FLOAT)
    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo("test", testCase.type, testCase.size);
      ByteBuffer byteBuffer = ByteBuffer.allocate(testCase.size).order(ByteOrder.LITTLE_ENDIAN);
      columnInfo.encode(byteBuffer, testCase.input);
      byteBuffer.flip();
      assertFalse(byteBuffer.hasRemaining(), "The bytebuffer should be empty");
    }));
  }

  Date date(String format, String date) {
    SimpleDateFormat f = new SimpleDateFormat(format);
    f.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      return f.parse(date);
    } catch (ParseException e) {
      throw new IllegalStateException("Exception thrown while parsing date", e);
    }
  }

  @TestFactory
  public Stream<DynamicTest> encode() throws ParseException {
    return Arrays.asList(
        of(1, VerticaColumnType.INTEGER, (byte) 1, "01"),
        of(2, VerticaColumnType.INTEGER, (short) 1, "0100"),
        of(4, VerticaColumnType.INTEGER,  1, "01000000"),
        of(8, VerticaColumnType.INTEGER, (long) 1, "0100000000000000"),
        of(8, VerticaColumnType.FLOAT, -1.11, "C3F5285C8FC2F1BF"),
        of(1, VerticaColumnType.BOOLEAN, Boolean.TRUE, "01"),
        of(-1, VerticaColumnType.VARCHAR, "ONE", "030000004F4E45"),
        of(10, VerticaColumnType.CHAR, "one       ", "6F6E6520202020202020"),
        of(-1, VerticaColumnType.VARBINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "08000000FFFFFFFFFFFFEF7F"),
        of(12, VerticaColumnType.BINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "FFFFFFFFFFFFEF7F00000000"),
        of(8, VerticaColumnType.DATE, new Date(915753600000L), "9AFEFFFFFFFFFFFF"),
        of(8, VerticaColumnType.TIMESTAMP, new Date(919739512350L), "3085B34F7EE7FFFF"),
        of(8, VerticaColumnType.TIMESTAMPTZ, date("yyyy-MM-dd HH:mm:ssX", "1999-01-08 07:04:37-05"), "401F3E64E8E3FFFF"),
        of(8, VerticaColumnType.TIME, date("HH:mm:ss", "07:09:23"), "C02E98FF05000000"),
//        of(8, VerticaColumnType.TIMETZ, date("HH:mm:ssX", "15:12:34-05"), "D0970180F079F010"),
        of("0000000000000000000000000000000064D6120000000000".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(1234532), "0000000000000000000000000000000064D6120000000000", 38, 0),
        of(8, VerticaColumnType.INTERVAL, (Duration.ofHours(3).plusMinutes(3).plusSeconds(3).toMillis() * 1000L), "C047A38E02000000")

    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo("test", testCase.type, testCase.size, testCase.precision, testCase.scale);
      assertEquals("test", columnInfo.name(), "name should match.");
      assertEquals(testCase.type, columnInfo.type(), "type should match.");
      assertEquals(testCase.size, columnInfo.size(), "size should match.");
      assertEquals(testCase.precision, columnInfo.precision(), "precision should match.");
      assertEquals(testCase.scale, columnInfo.scale(), "scale should match.");
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
      columnInfo.encode(byteBuffer, testCase.input);
      byteBuffer.flip();
      if (null != testCase.input) {
        assertTrue(byteBuffer.hasRemaining(), "The byteBuffer should have something in it.");
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.get(buffer);
        final String actual = BaseEncoding.base16().encode(buffer);
        assertEquals(testCase.expectedValue, actual, "output does not match.");
      } else {
        assertFalse(byteBuffer.hasRemaining(), "The byteBuffer should be empty");
      }
    }));
  }
}

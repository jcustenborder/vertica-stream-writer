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

import com.google.common.base.MoreObjects;
import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
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
    Map<VerticaType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaType.BOOLEAN, 1);
    testcases.put(VerticaType.FLOAT, 8);
    testcases.put(VerticaType.DATE, 8);
    testcases.put(VerticaType.TIME, 8);
    testcases.put(VerticaType.TIMETZ, 8);
    testcases.put(VerticaType.TIMESTAMP, 8);
    testcases.put(VerticaType.TIMESTAMPTZ, 8);
    testcases.put(VerticaType.INTERVAL, 8);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals((int) entry.getValue(), columnInfo.size, "size should match.");
    }));
  }

  @TestFactory
  public Stream<DynamicTest> variableSize() {
    Map<VerticaType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaType.VARBINARY, 1234);
    testcases.put(VerticaType.VARCHAR, 56442);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals(-1, columnInfo.size, "size should match.");
    }));
  }

  @TestFactory
  public Stream<DynamicTest> userDefinedSize() {
    Map<VerticaType, Integer> testcases = new LinkedHashMap<>();
    testcases.put(VerticaType.CHAR, 1234);
    testcases.put(VerticaType.BINARY, 56442);

    return testcases.entrySet().stream().map(entry -> dynamicTest(entry.getKey().toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo(entry.getKey().toString(), entry.getKey(), entry.getValue());
      assertEquals(entry.getKey().toString(), columnInfo.name, "name should match.");
      assertEquals(entry.getKey(), columnInfo.type, "type should match.");
      assertEquals((int) entry.getValue(), columnInfo.size, "size should match.");
    }));
  }

  static class EncodeTestCase {

    final int size;
    final VerticaType type;
    final Object input;
    final String expectedValue;
    final int precision;
    final int scale;


    EncodeTestCase(int size, VerticaType type, Object input, String expectedValue, int precision, int scale) {
      this.size = size;
      this.type = type;
      this.input = input;
      this.expectedValue = expectedValue;
      this.precision = precision;
      this.scale = scale;
    }

    EncodeTestCase(int size, VerticaType type, Object input, String expectedValue) {
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
      VerticaType type,
      Object input,
      String expectedValue) {
    return new EncodeTestCase(size, type, input, expectedValue);
  }

  static EncodeTestCase of(
      int size,
      VerticaType type,
      Object input,
      String expectedValue,
      int precision,
      int scale) {
    return new EncodeTestCase(size, type, input, expectedValue, precision, scale);
  }

  static EncodeTestCase nulls(int size, VerticaType type) {
    return of(size, type, null, null);
  }

  @TestFactory
  public Stream<DynamicTest> encodeNulls() {
    return Arrays.asList(
        nulls(1, VerticaType.INTEGER),
        nulls(2, VerticaType.INTEGER),
        nulls(4, VerticaType.INTEGER),
        nulls(8, VerticaType.INTEGER),
        nulls(8, VerticaType.FLOAT)
    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo("test", testCase.type, testCase.size);
      ByteBuffer byteBuffer = ByteBuffer.allocate(testCase.size).order(ByteOrder.LITTLE_ENDIAN);
      columnInfo.encode(byteBuffer, testCase.input);
      byteBuffer.flip();
      assertFalse(byteBuffer.hasRemaining(), "The bytebuffer should be empty");
    }));
  }

  @Test
  public void foo() {
    ByteBuffer buffer = ByteBuffer.wrap(BaseEncoding.base16().decode("D0970180F079F010")).order(ByteOrder.LITTLE_ENDIAN);
    log.trace("{}", buffer.getLong());
    buffer = ByteBuffer.wrap(BaseEncoding.base16().decode("6601000000000000")).order(ByteOrder.LITTLE_ENDIAN);
    log.trace("{}", buffer.getLong());
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
        of(1, VerticaType.INTEGER, 1, "01"),
        of(2, VerticaType.INTEGER, 1, "0100"),
        of(4, VerticaType.INTEGER, 1, "01000000"),
        of(8, VerticaType.INTEGER, 1, "0100000000000000"),
        of(8, VerticaType.FLOAT, -1.11, "C3F5285C8FC2F1BF"),
        of(1, VerticaType.BOOLEAN, Boolean.TRUE, "01"),
        of(-1, VerticaType.VARCHAR, "ONE", "030000004F4E45"),
        of(10, VerticaType.CHAR, "one       ", "6F6E6520202020202020"),
        of(-1, VerticaType.VARBINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "08000000FFFFFFFFFFFFEF7F"),
        of(12, VerticaType.BINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "FFFFFFFFFFFFEF7F00000000"),
        of(8, VerticaType.DATE, new Date(915753600000L), "9AFEFFFFFFFFFFFF"),
        of(8, VerticaType.TIMESTAMP, new Date(919739512350L), "3085B34F7EE7FFFF"),
        of(8, VerticaType.TIMESTAMPTZ, date("yyyy-MM-dd HH:mm:ssX", "1999-01-08 07:04:37-05"), "401F3E64E8E3FFFF"),
        of(8, VerticaType.TIME, date("HH:mm:ss", "07:09:23"), "C02E98FF05000000"),
        of(8, VerticaType.TIMETZ, date("HH:mm:ssX", "15:12:34-05"), "D0970180F079F010"),
        of("0000000000000000000000000000000064D6120000000000".length() / 2, VerticaType.NUMERIC, BigDecimal.valueOf(1234532), "0000000000000000000000000000000064D6120000000000", 38, 0),
        of(8, VerticaType.INTERVAL, (Duration.ofHours(3).plusMinutes(3).plusSeconds(3).toMillis() * 1000L), "C047A38E02000000")

    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      VerticaColumnInfo columnInfo = new VerticaColumnInfo("test", testCase.type, testCase.size, testCase.precision, testCase.scale);
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

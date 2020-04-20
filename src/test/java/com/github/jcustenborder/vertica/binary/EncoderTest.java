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
package com.github.jcustenborder.vertica.binary;

import com.github.jcustenborder.vertica.VerticaColumnType;
import com.google.common.base.MoreObjects;
import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class EncoderTest {

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

  Encoders encoders;

  @BeforeEach
  public void before() {
    this.encoders = new Encoders();
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
        of(4, VerticaColumnType.INTEGER, 1, "01000000"),
        of(8, VerticaColumnType.INTEGER, (long) 1, "0100000000000000"),
        of(8, VerticaColumnType.FLOAT, -1.11, "C3F5285C8FC2F1BF"),
        of(1, VerticaColumnType.BOOLEAN, Boolean.TRUE, "01"),
        of(-1, VerticaColumnType.VARCHAR, "ONE", "030000004F4E45"),
        of(10, VerticaColumnType.CHAR, "one       ", "6F6E6520202020202020"),
        of(-1, VerticaColumnType.VARBINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "08000000FFFFFFFFFFFFEF7F"),
//        of(-1, VerticaColumnType.VARBINARY, ByteBuffer.wrap(BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F")), "08000000FFFFFFFFFFFFEF7F"),
        of(12, VerticaColumnType.BINARY, BaseEncoding.base16().decode("FFFFFFFFFFFFEF7F"), "FFFFFFFFFFFFEF7F00000000"),
        of(8, VerticaColumnType.DATE, new Date(915753600000L), "9AFEFFFFFFFFFFFF"),
        of(8, VerticaColumnType.DATE, new java.sql.Date(915753600000L), "9AFEFFFFFFFFFFFF"),
        of(8, VerticaColumnType.TIMESTAMP, new Date(919739512350L), "3085B34F7EE7FFFF"),
        of(8, VerticaColumnType.TIMESTAMP, new java.sql.Date(919739512350L), "3085B34F7EE7FFFF"),
        of(8, VerticaColumnType.TIMESTAMP, new java.sql.Timestamp(919739512350L), "3085B34F7EE7FFFF"),
        of(8, VerticaColumnType.TIMESTAMP, LocalDateTime.ofInstant(new Date(919739512350L).toInstant(), ZoneId.of("UTC")), "3085B34F7EE7FFFF"),
        of(8, VerticaColumnType.TIMESTAMPTZ, date("yyyy-MM-dd HH:mm:ssX", "1999-01-08 07:04:37-05"), "401F3E64E8E3FFFF"),
        of(8, VerticaColumnType.TIME, date("HH:mm:ss", "07:09:23"), "C02E98FF05000000"),
//        of(8, VerticaColumnType.TIMETZ, LocalTime.of(15, 12, 34).atOffset(ZoneOffset.ofHours(-5)), "D0970180F079F010"),
        of("0000000000000000000000000000000064D6120000000000".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(1234532), "0000000000000000000000000000000064D6120000000000", 38, 0),
        of("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF9CFFFFFFFFFFFFFF".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(-1.0), "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF9CFFFFFFFFFFFFFF", 38, 2),
        of("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFDD4D56E0D5FFFFFF".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(-1809198413.15), "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFDD4D56E0D5FFFFFF", 38, 2),
        of("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFD3AA2C9FBFFFFFF".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(-1809198413.15), "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFD3AA2C9FBFFFFFF", 38, 1),
        of("06000000000000008056F30D9B9F4EB1".length() / 2, VerticaColumnType.NUMERIC, BigDecimal.valueOf(123456789123456789.123), "06000000000000008056F30D9B9F4EB1", 21, 3),
        of(8, VerticaColumnType.INTERVAL, (Duration.ofHours(3).plusMinutes(3).plusSeconds(3).toMillis() * 1000L), "C047A38E02000000"),
        of(8, VerticaColumnType.INTERVAL, Duration.ofHours(3).plusMinutes(3).plusSeconds(3), "C047A38E02000000")

    ).stream().map(testCase -> dynamicTest(testCase.toString(), () -> {
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
      Encoder encoder = this.encoders.get(testCase.type, testCase.input);
      assertNotNull(encoder, "Encoder was not returned.");
      encoder.encode(byteBuffer, testCase.input, "test", testCase.size, testCase.scale);

      byteBuffer.flip();
      assertTrue(byteBuffer.hasRemaining(), "The byteBuffer should have something in it.");
      byte[] buffer = new byte[byteBuffer.remaining()];
      byteBuffer.get(buffer);
      final String actual = BaseEncoding.base16().encode(buffer);
      assertEquals(testCase.expectedValue, actual, "output does not match.");
    }));
  }

}

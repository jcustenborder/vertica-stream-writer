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

import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VerticaBinaryStreamWriterTest {
  private static final Logger log = LoggerFactory.getLogger(VerticaBinaryStreamWriterTest.class);

  Date date(String format, String date) {
    SimpleDateFormat f = new SimpleDateFormat(format);
    f.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      return f.parse(date);
    } catch (ParseException e) {
      throw new IllegalStateException("Exception thrown while parsing date", e);
    }
  }

  @Test
  public void foo() throws IOException {
    VerticaStreamWriterBuilder builder = new VerticaStreamWriterBuilder()
        .table("allTypes")
        .column("INTCOL", VerticaColumnType.INTEGER, 8)
        .column("FLOATCOL", VerticaColumnType.FLOAT)
        .column("CHARCOL", VerticaColumnType.CHAR, 10)
        .column("VARCHARCOL", VerticaColumnType.VARCHAR)
        .column("BOOLCOL", VerticaColumnType.BOOLEAN)
        .column("DATECOL", VerticaColumnType.DATE)
        .column("TIMESTAMPCOL", VerticaColumnType.TIMESTAMP)
        .column("TIMESTAMPTZCOL", VerticaColumnType.TIMESTAMPTZ)
        .column("TIMECOL", VerticaColumnType.TIME)
        .column("TIMETZCOL", VerticaColumnType.TIMETZ)
        .column("VARBINCOL", VerticaColumnType.VARBINARY)
        .column("BINCOL", VerticaColumnType.BINARY, 3)
        .column("NUMCOL", VerticaColumnType.NUMERIC, 38, 0)
        .column("INTERVALCOL", VerticaColumnType.INTERVAL);

    Object[] row = new Object[]{
        1,
        -1.11D,
        "one       ",
        "ONE",
        true,
        new Date(915753600000L),
        new Date(919739512350L),
        date("yyyy-MM-dd HH:mm:ssX", "1999-01-08 07:04:37-05"),
        date("HH:mm:ss", "07:09:23"),
        date("HH:mm:ssX", "15:12:34-05"),
        BaseEncoding.base16().decode("ABCD"),
        BaseEncoding.base16().decode("ABCD"),
        BigDecimal.valueOf(1234532),
        (Duration.ofHours(3).plusMinutes(3).plusSeconds(3).toMillis() * 1000L)
    };

    assertEquals(14, builder.columnInfos.size(), "column count should match.");

    final String actual;

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      try (VerticaStreamWriter streamWriter = builder.build(outputStream)) {
        streamWriter.write(row);
      }
      actual = BaseEncoding.base16().encode(outputStream.toByteArray());
    }

    final String expected = "4E41544956450AFF0D0A003D0000000100000E0008000000080000000A000000FFFFFFFF010000000800000008000000080000000800000008000000FFFFFFFF0300000018000000080000007300000000000100000000000000C3F5285C8FC2F1BF6F6E6520202020202020030000004F4E45019AFEFFFFFFFFFFFF3085B34F7EE7FFFF401F3E64E8E3FFFFC02E98FF05000000D0970180F079F01002000000ABCDABCD000000000000000000000000000000000064D6120000000000C047A38E02000000";

    assertEquals(expected, actual);
  }

  String bitString(byte[] bytes) {
    StringBuilder builder = new StringBuilder();

    for (byte b : bytes) {
      builder.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
    }

    return builder.toString();
  }

}

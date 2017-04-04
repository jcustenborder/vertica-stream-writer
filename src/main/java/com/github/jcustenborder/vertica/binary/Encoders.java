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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Encoders {
  private static final Logger log = LoggerFactory.getLogger(Encoders.class);
  final Map<EncoderKey, Encoder> encoders;

  public Encoders() {
    List<Encoder> encoders = Arrays.asList(
        new VarCharStringEncoder(),
        new VarBinaryByteArrayEncoder(),
        new VarBinaryByteBufferEncoder(),
        new IntegerByteEncoder(),
        new IntegerShortEncoder(),
        new IntegerIntegerEncoder(),
        new IntegerLongEncoder(),
        new FloatDoubleEncoder(),
        new FloatFloatEncoder(),
        new DateUtilDateEncoder(),
        new DateSQLDateEncoder(),
        new CharStringEncoder(),
        new BooleanBooleanEncoder(),
        new BinaryByteArrayEncoder(),
        new IntervalDurationEncoder(),
        new IntervalLongEncoder(),
        new NumericBigDecimalEncoder(),
        new TimestampSQLTimestampEncoder(),
        new TimestampUtilDateEncoder(),
        new TimestampLocalDateTimeEncoder(),
        new TimestampSQLDateEncoder(),
        new TimeSQLTimeEncoder(),
        new TimeUtilDateEncoder(),
        new TimestampTZZonedDateTimeEncoder(),
        new TimestampTZSQLDateEncoder(),
        new TimestampTZUtilDateEncoder(),
        new TimeTZOffsetTimeEncoder()
    );

    this.encoders = ImmutableMap.copyOf(
        Maps.uniqueIndex(encoders, EncoderKey::of)
    );

    this.encoders.entrySet().stream().forEach(entry -> {
      log.trace("ctor() - Mapping {} to {}", entry.getKey(), entry.getValue());
    });
  }

  public Encoder get(VerticaColumnType columnType, Object value) {
    Preconditions.checkNotNull(value, "value cannot be null.");
    EncoderKey key = EncoderKey.of(value.getClass(), columnType);
    return this.encoders.get(key);
  }

}

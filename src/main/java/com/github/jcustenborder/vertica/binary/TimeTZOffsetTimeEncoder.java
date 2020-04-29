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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

class TimeTZOffsetTimeEncoder extends Encoder<OffsetTime> {
  private static final Logger log = LoggerFactory.getLogger(TimeTZOffsetTimeEncoder.class);

  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.TIMETZ;
  }

  @Override
  public Class<OffsetTime> inputType() {
    return OffsetTime.class;
  }

  @Override
  public void encode(
      ByteBuffer buffer,
      OffsetTime input,
      String name,
      int size,
      int precision,
      int scale
  ) {
    OffsetTime midnight = OffsetTime.of(LocalTime.MIDNIGHT, ZoneOffset.UTC);
    OffsetTime utcAdjust = input.withOffsetSameInstant(ZoneOffset.UTC);
    Duration duration = Duration.between(midnight, utcAdjust);
    log.trace("duration= {}", duration.getSeconds());
    log.trace("input = {}", input);
    final long microseconds = TimeUnit.HOURS.toMicros(utcAdjust.getHour()) +
        TimeUnit.MINUTES.toMicros(utcAdjust.getMinute()) +
        TimeUnit.SECONDS.toMicros(utcAdjust.getSecond()) +
        TimeUnit.NANOSECONDS.toMicros(utcAdjust.getNano());

//    final int
    final long offset = TimeUnit.HOURS.toSeconds(24) + input.getOffset().getTotalSeconds(); // ;
    log.trace("microseconds = {} offset = {}", microseconds, offset);
    final long storage = (microseconds << 24) + offset;
    log.trace("storage = {}", storage);
    buffer.putLong(storage);
  }
}

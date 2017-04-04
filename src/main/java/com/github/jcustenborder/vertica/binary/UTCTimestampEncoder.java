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

import com.github.jcustenborder.vertica.Constants;
import com.github.jcustenborder.vertica.VerticaColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.TimeZone;

public abstract class UTCTimestampEncoder<T> extends Encoder<T> {
  private static final Logger log = LoggerFactory.getLogger(UTCTimestampEncoder.class);

  static final TimeZone TIME_ZONE_UTC = TimeZone.getTimeZone("UTC");
  static final ZoneId TIME_ZONE_UTC_ZONEID = TimeZone.getTimeZone("UTC").toZoneId();


  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.TIMESTAMP;
  }

  protected abstract long utcTime(T input);

  @Override
  public void encode(ByteBuffer buffer, T input, String name, int size, int scale) {
    log.trace("input = {}", input);
    long utcTime = utcTime(input);
    long storage = (utcTime * 1000L - Constants.THEIR_EPOCH_MICRO);
    log.trace("storage = {}", storage);
    buffer.putLong(storage);
  }
}

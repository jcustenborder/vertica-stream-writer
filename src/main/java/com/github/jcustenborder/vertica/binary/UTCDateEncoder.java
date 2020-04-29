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

public abstract class UTCDateEncoder<T> extends Encoder<T> {
  private static final Logger log = LoggerFactory.getLogger(UTCDateEncoder.class);

  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.DATE;
  }

  protected abstract long utcTime(T input);

  @Override
  public void encode(
      ByteBuffer buffer,
      T input,
      String name,
      int size,
      int precision,
      int scale
  ) {
    log.trace("input = {}", input);
    long utcTime = utcTime(input);
    long storage = (utcTime - Constants.THEIR_EPOCH) / (1000 * 60 * 60 * 24);
    buffer.putLong(storage);
  }
}

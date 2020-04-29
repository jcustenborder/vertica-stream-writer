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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

class IntegerByteEncoder extends Encoder<Byte> {
  private static final Logger log = LoggerFactory.getLogger(IntegerByteEncoder.class);

  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.INTEGER;
  }

  @Override
  public Class<Byte> inputType() {
    return Byte.class;
  }

  @Override
  public void encode(
      ByteBuffer buffer,
      Byte input,
      String name,
      int size,
      int precision,
      int scale
  ) {
    Preconditions.checkState(size == 1, "Expected size should be 1.");
    log.trace("input = {}", input);
    buffer.put(input);
  }
}

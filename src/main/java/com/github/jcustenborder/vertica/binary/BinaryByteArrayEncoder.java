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
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BinaryByteArrayEncoder extends Encoder<byte[]> {
  private static final Logger log = LoggerFactory.getLogger(BinaryByteArrayEncoder.class);

  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.BINARY;
  }

  @Override
  public Class<byte[]> inputType() {
    return byte[].class;
  }

  @Override
  public void encode(
      ByteBuffer buffer,
      byte[] input,
      String name,
      int size,
      int precision,
      int scale
  ) {
    log.trace("input = {}", input);

    byte[] valueBuffer = (byte[]) input;

    Preconditions.checkState(
        size >= valueBuffer.length,
        "Encoded input for '%s' is %s byte(s) but the column is only %s byte(s).",
        name,
        valueBuffer.length,
        size
    );

    buffer.put(valueBuffer);
    int padding = size - valueBuffer.length;
    log.trace("padding input by {} byte(s).");
    for (int i = 0; i < padding; i++) {
      buffer.put(Constants.FALSE);
    }
  }
}

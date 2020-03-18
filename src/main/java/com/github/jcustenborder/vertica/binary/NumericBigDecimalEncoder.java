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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class NumericBigDecimalEncoder extends Encoder<BigDecimal> {
  private static final Logger log = LoggerFactory.getLogger(NumericBigDecimalEncoder.class);

  @Override
  public VerticaColumnType columnType() {
    return VerticaColumnType.NUMERIC;
  }

  @Override
  public Class<BigDecimal> inputType() {
    return BigDecimal.class;
  }

  @Override
  public void encode(ByteBuffer buffer, BigDecimal input, String name, int size, int scale) {
    /*
    This method needs some love. I'm not super familiar with what is going on here but I'm getting a correct value
    based on the document
     */
    log.trace("input = {}", input);

    //only accept the input if it's scale is lesser than or equal to the scale of Vertica column.
    Preconditions.checkState(
        scale >= input.scale(),
        "Scale for '%s' is mismatched. Value(%s) does not match definition of %s.",
        input.scale(),
        scale
    );

    final BigInteger unscaled = input.unscaledValue();
    byte[] unscaledBuffer = unscaled.toByteArray();
    log.trace("bufferSize:{}", size);
    //Allocate an extra byte to the buffer for accommodating the scale
    ByteBuffer byteBuffer = ByteBuffer.allocate(size + 1).order(ByteOrder.LITTLE_ENDIAN);
    final int bufferMinusScale = size - 5;
    final int paddingNeeded = bufferMinusScale - unscaledBuffer.length;
    log.trace("Padding with {} byte(s).", paddingNeeded);
    for (int i = 0; i < paddingNeeded; i++) {
      byteBuffer.put(Constants.ZERO);
    }
    for (int i = unscaledBuffer.length - 1; i >= 0; i--) {
      byteBuffer.put(unscaledBuffer[i]);
    }
    byteBuffer.put(Constants.ZERO);
    byteBuffer.putInt(scale);
    byteBuffer.flip();
    buffer.put(byteBuffer);

  }
}

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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private void littleEndianPut(ByteBuffer byteBuffer, byte[] src) {
    for (int i = src.length - 1; i >= 0; i--) {
      byteBuffer.put(src[i]);
    }
  }

  private void negate(byte[] bytes, int head) {
    for (int i = 0; i < head; i++) {
      bytes[i] ^= 0xFF;
    }
  }

  public void encode(ByteBuffer buffer, BigDecimal input, String name, int size, int scale) {
    /*
    This method needs some love. I'm not super familiar with what is going on here but I'm getting a correct value
    based on the document
     */
    log.trace("input = {}", input);

    // scale it aptly
    BigInteger unscaled = input
        .multiply(BigDecimal.valueOf(Math.pow(10, scale)))
        .toBigInteger();
    byte[] unscaledBuffer = unscaled.toByteArray();
    final int bufLen = unscaledBuffer.length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);

    // pad the input bytes
    byte[] paddedInput = new byte[size];
    System.arraycopy(unscaledBuffer, 0, paddedInput, size - bufLen, bufLen);

    log.trace("bufferSize:{}", size);

    // if negative value, take 2's complement
    if (input.signum() < 0) {
      negate(paddedInput, size - bufLen);
    }

    // go in chunks, each chunk being put as LE
    for (int k = 0; k < size / 8; k++) {
      // create an 8 byte word chunk
      byte[] chunk = Arrays.copyOfRange(paddedInput, k * 8, (k + 1) * 8);
      // put the chunk in LE fashion
      littleEndianPut(byteBuffer, chunk);
    }

    byteBuffer.flip();
    buffer.put(byteBuffer);
  }
}

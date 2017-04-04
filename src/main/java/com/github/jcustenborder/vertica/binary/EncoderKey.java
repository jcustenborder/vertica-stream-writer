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
import com.google.common.collect.ComparisonChain;

import java.util.Objects;

class EncoderKey implements Comparable<EncoderKey> {
  final Class inputType;
  final VerticaColumnType columnType;

  EncoderKey(Class inputType, VerticaColumnType columnType) {
    this.inputType = inputType;
    this.columnType = columnType;
  }

  public static EncoderKey of(Class inputType, VerticaColumnType columnType) {
    return new EncoderKey(inputType, columnType);
  }

  public static EncoderKey of(Encoder encoder) {
    return of(encoder.inputType(), encoder.columnType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.columnType, this.inputType);
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnType", this.columnType)
        .add("inputType", this.inputType.getName())
        .toString();
  }

  @Override
  public int compareTo(EncoderKey that) {
    return ComparisonChain.start()
        .compare(this.columnType, that.columnType)
        .compare(this.inputType.hashCode(), that.inputType.hashCode())
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EncoderKey) {
      return 0 == compareTo((EncoderKey) obj);
    } else {
      return false;
    }
  }
}

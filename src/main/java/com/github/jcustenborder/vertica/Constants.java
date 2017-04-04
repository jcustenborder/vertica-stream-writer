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
package com.github.jcustenborder.vertica;

import java.util.TimeZone;

public class Constants {



  private Constants() {

  }

  public final static TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
  public static final byte TRUE = (byte) 0x01;
  public static final byte FALSE = (byte) 0x00;
  public static final byte ZERO = FALSE;
  public static final long THEIR_EPOCH = 946684800000L;
  public static final long THEIR_EPOCH_MICRO = THEIR_EPOCH * 1000L;
}

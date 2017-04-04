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

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public interface VerticaStreamWriter extends Closeable {
  /**
   * Method is used to write a row to the stream.
   * @param row Array containing the objects for a row.
   * @throws IOException Exception thrown where there is an issue writing to the backing stream.
   * @exception IllegalStateException Exception thrown if the number of elements in the array do not match the number of columns defined.
   */
  void write(Object[] row) throws IOException;
}

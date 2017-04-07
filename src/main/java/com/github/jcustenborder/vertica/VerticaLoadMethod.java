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

public enum VerticaLoadMethod {
  /**
   * Loads data into WOS. Use this default COPY load method for smaller bulk loads.
   */
  AUTO,
  /**
   * Loads data directly into ROS containers. Use the DIRECT load method for large bulk loads (100MB or more).
   */
  DIRECT,
  /**
   * Loads data only into WOS. Use for frequent incremental loads, after the initial bulk load is complete.
   */
  TRICKLE
}

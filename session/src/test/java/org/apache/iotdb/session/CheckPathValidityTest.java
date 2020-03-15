/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session;

import static org.apache.iotdb.session.Config.PATH_PATTERN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CheckPathValidityTest {

  @Test
  public void testCheckPathValidity() {
    assertTrue(PATH_PATTERN.matcher("root.vehicle").matches());
    assertTrue(PATH_PATTERN.matcher("root.123456").matches());
    assertTrue(PATH_PATTERN.matcher("root._1234").matches());
    assertTrue(PATH_PATTERN.matcher("root._vehicle").matches());
    assertTrue(PATH_PATTERN.matcher("root.1234a4").matches());
    assertTrue(PATH_PATTERN.matcher("root.1_2").matches());
    assertTrue(PATH_PATTERN.matcher("root.vehicle.1245.1.2.3").matches());
    assertTrue(PATH_PATTERN.matcher("root.vehicle.1245.\"1.2.3\"").matches());
    assertTrue(PATH_PATTERN.matcher("root.vehicle.1245.\'1.2.3\'").matches());

    assertFalse(PATH_PATTERN.matcher("vehicle").matches());
    assertFalse(PATH_PATTERN.matcher("root.\tvehicle").matches());
    assertFalse(PATH_PATTERN.matcher("root.\nvehicle").matches());
    assertFalse(PATH_PATTERN.matcher("root..vehicle").matches());
    assertFalse(PATH_PATTERN.matcher("root.%12345").matches());
    assertFalse(PATH_PATTERN.matcher("root.a{12345}").matches());
  }
}

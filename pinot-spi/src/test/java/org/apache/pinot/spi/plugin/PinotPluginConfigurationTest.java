/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.plugin;

import java.util.List;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotPluginConfigurationTest {

  // A4: importFrom.* values must be split on "," with surrounding whitespace trimmed,
  // empty entries dropped, and parent.realmId trimmed.
  @Test
  public void testImportFromWithWhitespaceAroundCommas() {
    Properties props = new Properties();
    // Values with spaces around commas and leading/trailing whitespace in entries
    props.setProperty("importFrom.pinot", " org.example.foo , org.example.bar , ");
    PinotPluginConfiguration config = new PinotPluginConfiguration(props);

    List<String> imports = config.getImportsFromPerRealm().get("pinot");
    Assert.assertNotNull(imports, "Expected imports for realm 'pinot'");
    Assert.assertEquals(imports.size(), 2, "Empty entry after trailing comma must be dropped");
    Assert.assertEquals(imports.get(0), "org.example.foo");
    Assert.assertEquals(imports.get(1), "org.example.bar");
  }

  @Test
  public void testImportFromAllEmptyEntries() {
    Properties props = new Properties();
    // Only commas — all entries are empty after trimming
    props.setProperty("importFrom.pinot", " , , ");
    PinotPluginConfiguration config = new PinotPluginConfiguration(props);

    List<String> imports = config.getImportsFromPerRealm().get("pinot");
    Assert.assertNotNull(imports);
    Assert.assertTrue(imports.isEmpty(), "All-empty entries must produce an empty list");
  }

  @Test
  public void testParentRealmIdIsTrimmed() {
    Properties props = new Properties();
    props.setProperty("parent.realmId", "  pinot  ");
    PinotPluginConfiguration config = new PinotPluginConfiguration(props);

    Assert.assertTrue(config.getParentRealmId().isPresent());
    Assert.assertEquals(config.getParentRealmId().get(), "pinot",
        "parent.realmId must be trimmed of surrounding whitespace");
  }

  @Test
  public void testParentRealmIdAbsent() {
    Properties props = new Properties();
    PinotPluginConfiguration config = new PinotPluginConfiguration(props);
    Assert.assertFalse(config.getParentRealmId().isPresent(),
        "Optional must be empty when parent.realmId is not set");
  }

  @Test
  public void testRealmKeyIsTrimmed() {
    Properties props = new Properties();
    // The realm name after "importFrom." also gets trimmed
    props.setProperty("importFrom. myRealm ", "org.example.pkg");
    PinotPluginConfiguration config = new PinotPluginConfiguration(props);

    // " myRealm " trimmed → "myRealm"
    Assert.assertNotNull(config.getImportsFromPerRealm().get("myRealm"),
        "Realm key extracted from importFrom.* property must be trimmed");
  }
}

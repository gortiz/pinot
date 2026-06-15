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
package org.apache.pinot.controller;

import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AllowAllAccessFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Regression tests for {@link BaseControllerStarter#loadAccessControlFactory(String)}. The loader
 * is realm-aware via {@link org.apache.pinot.spi.plugin.PluginManager#loadClass(String)}, so
 * plugin-realm-resident factories work the same as in-tree ones — but the non-public-constructor
 * contract from the historical {@code Class.forName(...).getDeclaredConstructor()} pattern must be
 * preserved.
 */
public class AccessControlFactoryLoaderTest {

  @Test
  public void loadDefaultClassReturnsAllowAllAccessFactory() throws Exception {
    AccessControlFactory factory =
        BaseControllerStarter.loadAccessControlFactory(AllowAllAccessFactory.class.getName());
    assertEquals(factory.getClass(), AllowAllAccessFactory.class);
  }

  @Test
  public void loadUnknownClassThrowsClassNotFoundException() {
    try {
      BaseControllerStarter.loadAccessControlFactory("org.apache.pinot.does.not.Exist");
      fail("Expected an exception for unknown class");
    } catch (Exception thrown) {
      // The helper re-throws from PluginManager which wraps in ClassNotFoundException or similar;
      // accept ClassNotFoundException at any level of the cause chain.
      boolean hasClassNotFound = false;
      Throwable t = thrown;
      while (t != null) {
        if (t instanceof ClassNotFoundException || t instanceof NoClassDefFoundError) {
          hasClassNotFound = true;
          break;
        }
        t = t.getCause();
      }
      assertTrue(hasClassNotFound,
          "Expected ClassNotFoundException in the cause chain, got: " + thrown);
    }
  }

  /**
   * Regression test: the historical loader used {@link Class#getDeclaredConstructor()} which finds
   * non-public no-arg constructors. The realm-aware migration must preserve that contract or
   * factories with package-private / protected constructors would silently break on upgrade.
   */
  @Test
  public void loadFactoryWithPackagePrivateConstructorSucceeds() throws Exception {
    AccessControlFactory factory =
        BaseControllerStarter.loadAccessControlFactory(PackagePrivateCtorFactory.class.getName());
    assertEquals(factory.getClass(), PackagePrivateCtorFactory.class);
  }

  /** Test fixture: factory whose no-arg constructor is package-private. */
  public static class PackagePrivateCtorFactory implements AccessControlFactory {
    PackagePrivateCtorFactory() {
    }

    @Override
    public AccessControl create() {
      throw new UnsupportedOperationException("test fixture");
    }
  }
}

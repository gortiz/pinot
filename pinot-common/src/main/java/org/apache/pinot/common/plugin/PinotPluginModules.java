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
package org.apache.pinot.common.plugin;

import com.google.inject.Module;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Discovers Guice [Module] instances contributed by Pinot plugins.
///
/// Each plugin classloader is walked with [ServiceLoader] for `Module`
/// implementations declared in
/// `META-INF/services/com.google.inject.Module`. Plugin authors register
/// their `Module` there. The plugin classloaders themselves come from
/// [PluginManager#getPluginClassLoaders()] and have already been wired so
/// that `com.google.inject`, `jakarta.inject`, and `javax.inject` resolve
/// to the same `Class<?>` objects in plugin and core code.
///
/// Iteration order is plugin-load order (LinkedHashMap in `PluginManager`),
/// which is stable across JVMs for a given plugins directory.
///
/// This class is stateless and only loads once per call — callers that need
/// caching should cache themselves.
public final class PinotPluginModules {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotPluginModules.class);

  private PinotPluginModules() {
  }

  /// Discovers every plugin-provided [Module] across all plugin classloaders
  /// known to the supplied [PluginManager]. A plugin classloader whose
  /// `ServiceLoader` lookup throws is logged and skipped.
  public static List<Module> discover(PluginManager pluginManager) {
    List<Module> modules = new ArrayList<>();
    for (ClassLoader pluginClassLoader : pluginManager.getPluginClassLoaders()) {
      try {
        for (Module module : ServiceLoader.load(Module.class, pluginClassLoader)) {
          LOGGER.info("Discovered Guice Module {} from plugin classloader {}",
              module.getClass().getName(), pluginClassLoader);
          modules.add(module);
        }
      } catch (Throwable t) {
        LOGGER.warn("Failed to load Guice Modules from plugin classloader {}", pluginClassLoader, t);
      }
    }
    return modules;
  }
}

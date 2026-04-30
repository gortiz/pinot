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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.plugin.PluginManager;


/// Builds Pinot's role-level Guice injectors.
///
/// Each Pinot role (broker, server, controller, minion) constructs a single
/// process-wide [Injector] at startup. The injector wires together:
///
/// 1. Role-level core modules supplied by the caller (e.g. the broker's
///    `PinotBrokerCoreModule`).
/// 2. Plugin-contributed [Module]s discovered by [PinotPluginModules].
///
/// Plugins run AFTER the core modules so plugin bindings can observe and
/// extend any `Multibinder` declared by the core. The OSS-default bindings
/// for an extension point therefore live in the core module and plugins
/// layer additional contributions on top.
///
/// This class is stateless. Each call builds a fresh injector and re-walks
/// every plugin classloader, so callers should invoke it once per role
/// process at startup and reuse the returned [Injector] for the lifetime of
/// the JVM.
public final class PinotInjectors {

  private PinotInjectors() {
  }

  /// Convenience overload: discovers plugin modules from
  /// [PluginManager#get()].
  public static Injector createWithPluginModules(Module... coreModules) {
    return createWithPluginModules(PluginManager.get(), coreModules);
  }

  /// Builds an [Injector] composed of the supplied core modules followed by
  /// every plugin-contributed [Module] discovered through `pluginManager`.
  /// Order is significant: core modules run first, plugins run after.
  public static Injector createWithPluginModules(PluginManager pluginManager, Module... coreModules) {
    Objects.requireNonNull(pluginManager, "pluginManager");
    Objects.requireNonNull(coreModules, "coreModules");
    List<Module> modules = new ArrayList<>(coreModules.length);
    for (int i = 0; i < coreModules.length; i++) {
      modules.add(Objects.requireNonNull(coreModules[i], "coreModules[" + i + "]"));
    }
    modules.addAll(PinotPluginModules.discover(pluginManager));
    return Guice.createInjector(modules);
  }
}

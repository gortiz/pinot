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
package org.apache.pinot.broker.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import java.util.Objects;
import org.apache.pinot.query.planner.rules.DefaultRuleSetCustomizer;
import org.apache.pinot.query.planner.rules.RuleSetCustomizer;
import org.apache.pinot.spi.env.PinotConfiguration;


/// Core Guice module for broker-specific bindings.
///
/// Today the module binds:
///
/// - The broker's [PinotConfiguration] as the singleton instance constructed
///   from the broker startup config. Any class created by this injector can
///   read role config via `@Inject PinotConfiguration`.
/// - A `Multibinder<RuleSetCustomizer>` seeded with [DefaultRuleSetCustomizer]
///   as the first contribution. Plugins layer additional `RuleSetCustomizer`
///   bindings on top from their own `Module`s; the multibinder's iteration
///   order is binding order, so plugins always observe a list pre-populated
///   with the OSS defaults.
///
/// Over time this module will grow to bind more broker singletons as Pinot's
/// DI footprint expands.
public class PinotBrokerCoreModule extends AbstractModule {

  private final PinotConfiguration _brokerConf;

  public PinotBrokerCoreModule(PinotConfiguration brokerConf) {
    _brokerConf = Objects.requireNonNull(brokerConf, "brokerConf");
  }

  @Override
  protected void configure() {
    bind(PinotConfiguration.class).toInstance(_brokerConf);

    // Calcite rule extension point for the multi-stage engine. PinotRuleSet
    // (auto-bound @Singleton) consumes the multibound set, applies all
    // customizers in iteration order, and exposes the frozen per-phase lists
    // to QueryEnvironment. DefaultRuleSetCustomizer is bound first so every
    // plugin customizer observes a list pre-populated with the OSS defaults.
    Multibinder<RuleSetCustomizer> ruleCustomizers = Multibinder.newSetBinder(binder(), RuleSetCustomizer.class);
    ruleCustomizers.addBinding().to(DefaultRuleSetCustomizer.class);
  }
}

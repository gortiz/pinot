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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.pinot.query.planner.rules.Phase;
import org.apache.pinot.query.planner.rules.PinotRuleSet;
import org.apache.pinot.query.planner.rules.RuleSetCustomizer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies bindings on [PinotBrokerCoreModule]: [PinotConfiguration]
/// availability for downstream `@Inject` consumers, and the
/// [RuleSetCustomizer] / [PinotRuleSet] extension point.
public class PinotBrokerCoreModuleTest {

  @Test
  public void ossDefaultsSeedEveryPhase() {
    Injector injector = Guice.createInjector(new PinotBrokerCoreModule(new PinotConfiguration()));
    PinotRuleSet ruleSet = injector.getInstance(PinotRuleSet.class);

    assertFalse(ruleSet.rulesFor(Phase.BASIC).isEmpty(), "BASIC should be populated");
    assertFalse(ruleSet.rulesFor(Phase.FILTER_PUSHDOWN).isEmpty(), "FILTER_PUSHDOWN should be populated");
    assertFalse(ruleSet.rulesFor(Phase.PROJECT_PUSHDOWN).isEmpty(), "PROJECT_PUSHDOWN should be populated");
    assertFalse(ruleSet.rulesFor(Phase.PRUNE).isEmpty(), "PRUNE should be populated");
    assertFalse(ruleSet.rulesFor(Phase.POST_LOGICAL).isEmpty(), "POST_LOGICAL should be populated");
    assertFalse(ruleSet.rulesFor(Phase.POST_LOGICAL_V2).isEmpty(), "POST_LOGICAL_V2 should be populated");
    assertFalse(ruleSet.rulesFor(Phase.POST_LOGICAL_ENRICHED_JOIN).isEmpty(),
        "POST_LOGICAL_ENRICHED_JOIN should be populated");
  }

  @Test
  public void pluginCustomizerCanAppendToBasicAfterOssDefaults() {
    // Capture baseline from a plain injector, then verify plugin appended one more rule.
    Injector baseInjector = Guice.createInjector(new PinotBrokerCoreModule(new PinotConfiguration()));
    PinotRuleSet baseRuleSet = baseInjector.getInstance(PinotRuleSet.class);
    int ossSize = baseRuleSet.rulesFor(Phase.BASIC).size();
    RelOptRule extraRule = baseRuleSet.rulesFor(Phase.BASIC).get(0);

    RuleSetCustomizer customizer = (phase, rules) -> {
      if (phase == Phase.BASIC) {
        rules.add(extraRule);
      }
    };

    Injector injector = Guice.createInjector(
        new PinotBrokerCoreModule(new PinotConfiguration()),
        new AbstractModule() {
          @Override
          protected void configure() {
            Multibinder.newSetBinder(binder(), RuleSetCustomizer.class).addBinding().toInstance(customizer);
          }
        });

    PinotRuleSet ruleSet = injector.getInstance(PinotRuleSet.class);
    assertEquals(ruleSet.rulesFor(Phase.BASIC).size(), ossSize + 1);
    assertSame(ruleSet.rulesFor(Phase.BASIC).get(ossSize), extraRule);
  }

  @Test
  public void pluginCustomizerCanRemoveOssDefault() {
    Injector baseInjector = Guice.createInjector(new PinotBrokerCoreModule(new PinotConfiguration()));
    PinotRuleSet baseRuleSet = baseInjector.getInstance(PinotRuleSet.class);
    // Pick the concrete rule class of the first OSS-default rule and remove every rule of that
    // exact class. Class identity is a stronger signal than toString() — Calcite rule names are
    // not guaranteed unique.
    Class<?> targetClass = baseRuleSet.rulesFor(Phase.BASIC).get(0).getClass();
    int ossSize = baseRuleSet.rulesFor(Phase.BASIC).size();
    long ossInstancesOfTarget = baseRuleSet.rulesFor(Phase.BASIC).stream()
        .filter(r -> r.getClass() == targetClass)
        .count();

    RuleSetCustomizer customizer = (phase, rules) -> {
      if (phase == Phase.BASIC) {
        rules.removeIf(r -> r.getClass() == targetClass);
      }
    };

    Injector injector = Guice.createInjector(
        new PinotBrokerCoreModule(new PinotConfiguration()),
        new AbstractModule() {
          @Override
          protected void configure() {
            Multibinder.newSetBinder(binder(), RuleSetCustomizer.class).addBinding().toInstance(customizer);
          }
        });

    PinotRuleSet ruleSet = injector.getInstance(PinotRuleSet.class);
    assertEquals(ruleSet.rulesFor(Phase.BASIC).size(), ossSize - (int) ossInstancesOfTarget);
    assertTrue(ruleSet.rulesFor(Phase.BASIC).stream().noneMatch(r -> r.getClass() == targetClass));
  }

  @Test
  public void pinotRuleSetIsSingletonAcrossLookups() {
    Injector injector = Guice.createInjector(new PinotBrokerCoreModule(new PinotConfiguration()));
    PinotRuleSet first = injector.getInstance(PinotRuleSet.class);
    PinotRuleSet second = injector.getInstance(PinotRuleSet.class);
    assertSame(first, second);
  }

  @Test
  public void brokerConfigIsAvailableForInjection() {
    PinotConfiguration brokerConf = new PinotConfiguration();
    Injector injector = Guice.createInjector(new PinotBrokerCoreModule(brokerConf));
    assertSame(injector.getInstance(PinotConfiguration.class), brokerConf);
  }
}

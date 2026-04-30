<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Dependency injection in Pinot

> **Status**: incremental rollout. Today (broker only) Pinot uses [Guice 7] to wire a
> small, growing set of singletons. This document captures the conventions all new
> Guice-related code must follow. As more roles (server, controller, minion) and
> more singletons migrate, this document evolves with them.

[Guice 7]: https://github.com/google/guice

## Why Guice

Pinot has accumulated several extension-point patterns (`*Factory.init(String)` with
reflection, `ServiceLoader` lookups, `static getDefault()` singletons). They are hard
to test, hard to extend from a plugin without patching OSS, and hide their
dependencies. Guice gives us:

- **Constructor injection** — every dependency is visible in the signature, no
  hidden static state.
- **One discovery mechanism** — plugins contribute bindings via a Guice `Module`
  instead of inventing per-feature config keys, `META-INF/services` files, or
  reflection-based factories.
- **Type-safe composition** — the binding graph is checked at injector
  construction; missing dependencies fail fast at startup, not on the first query.

Guice's footprint is intentionally small: one `Injector` per role process, no AOP,
no scopes beyond `@Singleton`, no field injection in production code. It runs above
the planner and stays out of the per-query hot path.

## Conventions

These rules apply to every new Guice-using class.

### 1. Constructor injection only

```java
@Singleton
public class FooFactory {
  private final Set<Foo> _foos;

  @Inject
  public FooFactory(Set<Foo> foos) {
    _foos = List.copyOf(foos);
  }
}
```

- Field injection is **forbidden in production code**. It hides dependencies, breaks
  immutability, and prevents `final` fields. Only `src/test/java` may use field
  injection if a test framework requires it.
- No setter injection.
- No `@Inject` on private constructors.

### 2. Constructors must be side-effect-free

A class created by Guice may be instantiated multiple times during testing (multiple
injectors in the same JVM). Constructors must not mutate global state, register with
external services, or start threads. Move side effects into a separately-callable
method that the bootstrapping code invokes once.

### 3. Scopes: `@Singleton` only

Pinot's roles are long-lived processes; almost every binding is a singleton. Do not
introduce request scope, query scope, or any other custom scope — pass per-request
state explicitly through method parameters (`QueryContext`, `RequestContext`, etc.).

### 4. No AOP, no method interception, no proxies

Guice supports method interception via `bindInterceptor(...)`. **Don't use it.** It
creates synthetic subclasses, complicates stack traces, and is a per-call
performance trap. If cross-cutting behavior is needed (metrics, tracing), wrap the
collaborator explicitly in a decorator class.

### 5. SPI types live in their semantic module, not in `pinot-spi`

`pinot-spi` is the slim, plugin-facing contract module — it must not depend on
Guice. Plugin-contributed Guice bindings (multibinders, modules) live in higher
modules (`pinot-common`, `pinot-broker`, …). Plugins still depend only on
`pinot-spi` plus any module that owns the SPI they implement.

### 6. Plugins contribute via a `Module` discovered through `ServiceLoader`

Each role's injector walks its plugin classloaders at startup with
`ServiceLoader.load(Module.class, pluginClassLoader)`. A plugin contributes by:

1. Adding a `Module` class.
2. Listing it in
   `src/main/resources/META-INF/services/com.google.inject.Module`.

Plugins must **not** shade Guice. The role classloader exports
`com.google.inject`, `jakarta.inject`, and `javax.inject` to plugin realms, so the
core and plugin Modules see the same `Class<?>` objects.

### 7. Multibinder iteration order is binding order

Guice's `Multibinder<T>` produces a `Set<T>` whose iteration order is the order in
which bindings were added. Within a single `Module`, that's source order. Across
multiple `Module`s, OSS-core bindings come first, plugin bindings after.

If your code depends on **specific** ordering (e.g. rewriter A must run before B),
either bind both in the same Module in the right order, or introduce an explicit
ordering hook on the SPI (`int order()`) — don't rely on incidental Set iteration.

### 8. Operational config keys are first-class, not legacy

Config keys that control runtime behavior (rule lists, thresholds, class name
overrides) are permanent operator levers — they let an operator hot-fix behavior by
editing config and restarting, without a binary release. When a class needs
operational config, route it through DI: bind `PinotConfiguration` into the injector
and let the class `@Inject PinotConfiguration` to read it. Don't recreate
config-reading boilerplate per-class.

## Worked example: Calcite rule customization

The canonical demo of these conventions is the multi-stage planner's Calcite rule
extension point.

| Type | Where | What |
| ---- | ----- | ---- |
| `Phase` | `pinot-query-planner/.../planner/rules/` | Top-level enum, append-only, one value per HEP slot |
| `RuleSetCustomizer` | same package | SPI: `void customize(Phase, List<RelOptRule>)` — plugins mutate the list in place |
| `PinotRuleSet` | same package | `@Singleton` service, `@Inject` ctor takes `Set<RuleSetCustomizer>`, applies them in iteration order, freezes per-phase lists |
| `DefaultRuleSetCustomizer` | same package | `RuleSetCustomizer` that owns and seeds every `Phase` with the OSS default rules. Bound first in `PinotBrokerCoreModule` so plugins observe pre-populated lists |
| `PinotBrokerCoreModule` | `pinot-broker/.../runtime/` | Binds `PinotConfiguration`, declares `Multibinder<RuleSetCustomizer>`, binds `DefaultRuleSetCustomizer` |
| `QueryEnvironment.Config` | `pinot-query-planner/.../query/` | `PinotRuleSet getRuleSet()` accessor; `getOptProgram` / `getTraitProgram` read every phase from it |

Plugins customize without patching OSS:

```java
public class MyPluginRules implements RuleSetCustomizer {
  @Override public void customize(Phase phase, List<RelOptRule> rules) {
    if (phase == Phase.BASIC) {
      rules.add(MyOptimizationRule.INSTANCE);
      rules.removeIf(r -> "BadOldRule".equals(r.toString()));
    }
  }
}

public class MyPluginModule extends AbstractModule {
  @Override protected void configure() {
    Multibinder.newSetBinder(binder(), RuleSetCustomizer.class)
        .addBinding().to(MyPluginRules.class);
  }
}
```

Per-query `usePlannerRules` / `skipPlannerRules` query options still apply on top via
the existing `QueryEnvironment#filterRuleList` (a copy + filter applied to the
result of `PinotRuleSet#rulesFor(phase)`).

## Where bindings live

| Module                | What it binds                                                                            |
| --------------------- | ---------------------------------------------------------------------------------------- |
| `pinot-spi`           | Nothing (Guice-free; plugin-classloader hooks only).                                      |
| `pinot-common`        | Generic helpers: `PinotInjectors.createWithPluginModules`, `PinotPluginModules.discover`. |
| `pinot-query-planner` | `RuleSetCustomizer` SPI, `Phase`, `PinotRuleSet`, `DefaultRuleSetCustomizer`.              |
| `pinot-broker`        | `PinotBrokerCoreModule` — broker singletons + `Multibinder<RuleSetCustomizer>`.           |
| `pinot-server`        | _(future)_ `PinotServerCoreModule`.                                                       |
| `pinot-controller`    | _(future)_ `PinotControllerCoreModule`.                                                   |
| `pinot-minion`        | _(future)_ `PinotMinionCoreModule`.                                                       |

Each role builds its injector once at startup with
`PinotInjectors.createWithPluginModules(coreModule)`.

## What this is not

- Not a replacement for `ServiceLoader` everywhere — `ServiceLoader` still discovers
  plugin `Module`s themselves; only what the modules contribute (`RuleSetCustomizer`,
  …) flows through Multibinders.
- Not a replacement for HK2/Jersey on the REST resource graph — Jersey continues to
  manage its own injection.
- Not a request-scoping framework — there is no per-query injector, no
  `@RequestScoped`. Per-query state is passed explicitly through method arguments.
- Not used in the per-row / per-segment hot path — adapters and operators are
  resolved at planning time, never at execution time.

## Backwards compatibility

- **Guice is role-local**: changes to a role's `*CoreModule` do not affect wire
  protocols or serialization. Mixed-version clusters can upgrade roles independently.
- **Plugins shipping their own shaded Guice** will fail at injector build time with
  `ClassCastException`. The fix is to remove the shade and depend on Pinot-provided
  Guice.
- **OSS default behavior must be preserved**: when seeding a new `Multibinder`, bind
  the OSS default implementation first so plugins always observe a pre-populated list.

## Adding a new binding (checklist)

When you add a new `@Inject`-annotated class or extend a `Multibinder`:

- [ ] Constructor only, no field injection (production).
- [ ] No mutation of global state in the constructor; if you must publish, expose
      a separate explicit method.
- [ ] `@Singleton` if process-lifetime; never request/query scope.
- [ ] Bound in the appropriate `*CoreModule`, or contributed via Multibinder if
      it's an extension-point set.
- [ ] If a new Multibinder type is added, declare an empty multibinder in the core
      module so `Set<T>` is always injectable.
- [ ] Tests run `Guice.createInjector(coreModule, testModule)` and assert behavior;
      never `static init(...)` from inside the binding's constructor.

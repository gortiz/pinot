# Plugin ClassRealm Model — Migration and Reference

This document describes the per-plugin classloader isolation model introduced on the
`pinot-no-shading` branch.  It replaces the previous approach of packaging every plugin
as a Maven-shaded fat-jar with relocated packages.

---

## 1. Overview

Previously, every built-in Pinot plugin was shaded: its dependencies were merged into a
single jar and their package names were rewritten to avoid conflicts with other plugins or
with the host classpath.  This was effective but fragile: relocation had to be maintained
manually, and debugging "which jackson are we actually using?" was painful.

The new model gives each plugin its own **ClassRealm** (a [Plexus ClassWorlds][classworlds]
classloader) that is populated with the plugin jar and its runtime dependency jars.
Realms are isolated by default — a class visible in plugin A is not visible in plugin B
unless an explicit import is declared.  `pinot-spi` is the exception: it is always
imported automatically.

Key properties:

- Each plugin directory becomes one ClassRealm named after the directory.
- `org.apache.pinot.spi.*` is always visible inside every plugin realm (auto-import from
  the `pinot` parent realm, which wraps the system classloader).
- A plugin opts into the realm model by shipping a `pinot-plugin.properties` file (inside
  its jar or alongside it).  Without the file, the old `PluginClassLoader` path is used
  for backward compatibility.
- No more relocation.  Plugins ship their actual dependency jars, unmodified.

[classworlds]: https://codehaus-plexus.github.io/plexus-classworlds/

---

## 2. `pinot-plugin.properties` Reference

Place this file in `src/main/resources/pinot-plugin.properties` of your plugin module.
`PluginManager` finds it inside the plugin jar at runtime; you do not need to place it
next to the jar in the distribution.

### Keys

| Key | Type | Description |
|-----|------|-------------|
| `parent.realmId` | String | Optional. Set to `pinot` to make the plugin realm fall back to the parent (Pinot core) classloader after exhausting its own URLs. Preserves access to `pinot-common` / `pinot-core` classes. Omit for strict isolation (only `pinot-spi` is visible). |
| `importFrom.<realmId>` | Comma-separated package prefixes | Optional. Import specific package prefixes from another named realm. Use when your plugin needs classes from a sibling plugin's realm rather than the host classpath. |

`org.apache.pinot.spi` is **always** imported from the `pinot` parent realm regardless of
whether you set `parent.realmId`.  You do not need to declare it.

Reserved realm names: `DEFAULT` and `pinot`.  Do not name your plugin directory either of
those names.

### Variants

#### Variant A — strict isolation (SPI-only plugins)

A plugin that depends only on `pinot-spi` can use an effectively empty file:

```properties
# pinot-plugin.properties — strict isolation
# org.apache.pinot.spi is always available; nothing else from core is needed.
```

The file's presence (even empty) is what activates the realm path.
`pinot-dropwizard` uses this form:

```properties
# no parent.realmId, which implies limited plugin
```

This is correct for Dropwizard because `PinotMetricsFactory` and all types passed across
the SPI boundary live under `org.apache.pinot.spi.*`, so the auto-import is sufficient.

#### Variant B — parent fallback (legacy-compatible plugins)

Plugins that use helper classes from `pinot-common` or `pinot-core` should set
`parent.realmId=pinot`.  This makes the realm delegate to the host classpath (the system
classloader, which has `pinot-all.jar` on it) when a class is not found in the plugin's
own URLs.  It matches the legacy `PluginClassLoader` parent-delegation behaviour and is
the safe migration path for older plugins.

`pinot-kafka-3.0` uses this form:

```properties
# parent.realmId=pinot makes this plugin's ClassRealm fall back to the parent (Pinot core)
# classloader when a class is not found in the plugin's own URLs. Matches the legacy
# PluginClassLoader's parent-first behaviour and preserves access to pinot-common /
# pinot-core helper classes that older plugins rely on. Plugins that genuinely only depend
# on pinot-spi can later switch to the limited-realm form (an empty file) for tighter
# isolation.
parent.realmId=pinot
```

#### Variant C — cross-realm import

Use `importFrom.<realmId>` when your plugin needs specific packages from a sibling plugin
realm.  The value is a comma-separated list of package-prefix strings (prefix-matched, not
glob):

```properties
# Import com.fasterxml.jackson.* from the pinot-json plugin realm
importFrom.pinot-json=com.fasterxml.jackson
```

This is only needed when you want to share live class instances across realm boundaries
(e.g. to avoid `ClassCastException` because two realms each loaded their own copy of the
same Jackson type).  For most plugins `parent.realmId=pinot` is the right choice instead.

---

## 3. On-Disk Layout in the Distribution

Plugin zips are produced by the `assembly-descriptor/pinot-plugin.xml` descriptor and
unpacked into the distribution's `plugins/` directory by `pinot-assembly.xml`.  After
unpacking, the layout is:

```
plugins/
  pinot-kafka-3.0/
    pinot-plugin.properties          ← metadata (activates realm mode)
    pinot-kafka-3.0-VERSION.jar      ← plugin jar (named after the module)
    kafka-clients-VERSION.jar        ← runtime dep
    kafka_2.13-VERSION.jar           ← runtime dep
    jackson-module-scala_...jar      ← runtime dep
    ...                              ← other runtime deps (flat, no subdirectory)
    classes/                         ← compiled .class files from the module itself
  pinot-dropwizard/
    pinot-plugin.properties
    pinot-dropwizard-VERSION.jar
    metrics-core-VERSION.jar
    metrics-jmx-VERSION.jar
    ...
```

Key points:

- `pinot-plugin.properties` sits at the **root** of the plugin directory (extracted there
  by the assembly descriptor's `<files>` section).
- Runtime dependency jars are **flat** next to the plugin jar — no subdirectories (except
  `classes/` which holds the module's own compiled classes).
- `PluginManager.load()` calls `Files.list(directory)` (non-recursive, top-level only) and
  adds every entry to the realm's URLs.  The `classes/` subdirectory is not walked
  recursively by default; the plugin jar is the primary vehicle for the plugin's own code.
- The distribution assembly (`pinot-assembly.xml`) collects plugins as `zip:plugin`
  artifacts with `<unpack>true</unpack>` and `<outputDirectory>plugins</outputDirectory>`,
  so each zip unpacks into `plugins/<pluginName>/` directly.
- External plugins that have not yet been converted (Hadoop, Spark batch ingestion) still
  ship as shaded jars under `plugins-external/` and are loaded via the legacy
  `PluginClassLoader` path.

---

## 4. Plugin POM — Dependency Scope Contract

The plugin assembly picks up dependencies at `scope=runtime` (which includes `compile`
and `runtime`-scoped deps, but excludes `provided` and `test`).

| Maven scope | Shipped in plugin zip | Visible at runtime |
|-------------|----------------------|--------------------|
| `compile` / `runtime` | Yes | Yes — on the realm's classpath |
| `provided` | No | Must be on the host classpath (`lib/pinot-all.jar`) |
| `test` | No | Test classpath only |

### When to use `provided`

Declare a dependency `provided` when the class is guaranteed to be on the host classpath
AND you want to avoid shipping a second copy inside the plugin (which would waste space and
potentially cause `ClassCastException` if two incompatible copies are loaded).

Example — `pinot-kafka-base/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.pinot</groupId>
  <artifactId>pinot-json</artifactId>
  <scope>provided</scope>   <!-- JSONMessageDecoder is on the host classpath via pinot-all -->
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <scope>provided</scope>   <!-- supplied by pinot-kafka-3.0 itself, not duplicated here -->
</dependency>
```

`pinot-json` is in the `pinot-json` plugin realm at runtime.  If `kafka-base` shipped its
own copy it would be a different class object from the host's copy, breaking any
`instanceof` check or cross-realm cast.  Declaring it `provided` avoids the duplication.

`pinot-spi` itself should never appear as a `compile`-scoped dep in a plugin POM; it is
always available via the auto parent-import and you would only bloat the plugin zip.

---

## 5. Resolving Classes by Name

### Bare FQCN — realm walk

```java
PluginManager.get().createInstance("com.example.MyRecordReader");
```

`PluginManager.loadClass(String)` sees no `:` separator and does an **unscoped walk**: it
tries the default `PluginClassLoader` first (parent is the system classloader), then every
registered `ClassRealm` in load order, then every legacy `PluginClassLoader` in
registration order.  The first match wins.

If more than one classloader exposes the same FQCN a `WARN` is logged:

```
Class com.example.Foo resolved on multiple plugin classloaders; using <first> (also found
on [<others>]). Use pluginName:className to disambiguate.
```

### Pinned form — `pluginName:FQCN`

```java
PluginManager.get().createInstance("pinot-kafka-3.0:org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
```

The part before `:` is the realm name (= the plugin directory name).  Only that realm is
consulted; the walk is not performed.  If the realm does not exist, a descriptive
`ClassNotFoundException` lists all known realms.

**When to use the pinned form:**

- When the same FQCN is visible in more than one realm and you need a specific one.
- When integrating with `--strict-realm` mode of `verify-plugins.sh`.
- When building tooling that needs deterministic class provenance.

For normal Pinot SPI usage (table config referencing a class name) the bare form is
correct — it works even if you do not know which plugin will provide the class.

---

## 6. Diagnosing Load Failures

### Reading the improved `ClassNotFoundException`

When a bare FQCN fails to resolve on any classloader, `PluginManager` throws:

```
ClassNotFoundException: Class 'com.example.Foo' not found on any of the 12 classloader(s)
searched: [PluginClassLoader@..., ClassRealm[pinot-kafka-3.0, ...], ...]
  Suppressed: NoClassDefFoundError: com/example/TransitiveDep
```

The message tells you:

- **How many classloaders were searched** — if the number is lower than expected, a plugin
  directory may be missing or failed to load during startup (check logs for
  `Failed to load plugin`).
- **Which classloaders were searched** — realm names are the plugin directory names.  If
  the realm you expected is absent, the plugin was either not found at startup or its
  `pinot-plugin.properties` was missing (which silently falls back to the legacy path).
- **Suppressed `NoClassDefFoundError`** — this is the key transitive-dependency signal.
  It means the plugin's bytecode was found (the plugin jar is present and loaded) but one
  of its dependency jars is missing from the realm.  The error message names the missing
  class; from there, find which jar provides that class and verify it is in the plugin
  directory.  The WARN log line preceding the exception also names the classloader that saw
  the `NoClassDefFoundError`.

Example diagnostic workflow:

1. See `NoClassDefFoundError: io/confluent/kafka/schemaregistry/client/SchemaRegistryClient`
   in the suppressed chain.
2. Identify that class as belonging to `confluent-kafka-avro-serializer-*.jar`.
3. Check whether that jar is present in `plugins/pinot-confluent-avro/`.
4. If absent, the dependency was not declared `compile`/`runtime` in the plugin POM, or
   the zip was built with `pinot-fastdev` active (which suppresses the shaded jar and may
   change assembly output).

### Plugin Verifier

For systematic pre-release checking, use the standalone verifier:

```bash
# Build the distribution first (disable pinot-fastdev to get proper plugin zips):
./mvnw -Pbin-dist -P!pinot-fastdev install -DskipTests

# Run all checks:
build/bin/verify-plugins.sh

# Run with verbose output to see which jar/realm each class came from:
build/bin/verify-plugins.sh build/ --verbose

# Pin to strict-realm mode (exercises the pluginName:FQCN path):
build/bin/verify-plugins.sh build/ --strict-realm
```

See [`pinot-plugin-verifier/README.md`](../pinot-plugin-verifier/README.md) for the full
flag reference and a description of what each check exercises.

> **Note:** Do not build with `-Ppinot-fastdev` when verifying the distribution.  That
> profile sets `shade.phase.prop=none`, which suppresses the per-plugin shaded jar that
> the assembly copies into `plugins/<name>/`.  Local environments that auto-activate it
> via `~/.m2/settings.xml` need an explicit `-P!pinot-fastdev`.

---

## 7. Checklist for Plugin Authors

When writing a new plugin or converting an existing one to the realm model:

- [ ] Add `src/main/resources/pinot-plugin.properties` (even if empty — its presence opts
      the plugin into realm loading).
- [ ] If the plugin only uses `org.apache.pinot.spi.*`, leave the file empty (or add a
      comment).  Do not add `parent.realmId=pinot` unless you genuinely need `pinot-core`
      or `pinot-common` classes.
- [ ] If the plugin uses `pinot-common` / `pinot-core` helper classes and cannot easily be
      refactored, add `parent.realmId=pinot`.
- [ ] Declare all runtime dependencies as `compile` or `runtime` scope (they must land in
      the plugin zip).  Declare `pinot-spi` as `provided` — it is always on the realm
      via auto-import, never needs to be shipped.
- [ ] Declare `provided` any dependency that is guaranteed to be on `pinot-all.jar` (the
      host classpath) AND whose class objects must be the same instance as the host's copy
      to avoid `ClassCastException`.
- [ ] Verify the assembled zip contains the expected jars:
      `unzip -l target/<artifact>-VERSION-plugin.zip`
- [ ] Run `build/bin/verify-plugins.sh build/ --verbose` against a distribution build to
      confirm the class is loaded from the plugin realm (not leaked from `pinot-all.jar`).

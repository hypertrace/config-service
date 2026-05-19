---
name: module-organization
type: domain-knowledge
---

With 100+ modules, where code lives matters. Reviewers regularly redirect code to its proper module.

## Rules the reviewers enforce

- **`TraceableConfigService` (the main service) should not host config-service-specific things.** Keep the main service generic. Domain-specific clients/configs go in the domain module. *"`TraceableConfigService` shouldn't host things that are specific to a specific config service. Pass the config to the child service."*
- **Pass channels, not stubs, to child services** when possible. This lets the child decide blocking vs future vs streaming. The exception is the generic config-service client which everyone uses.
- **Don't import another domain's module directly to reuse a single client/util.** Reviewers push back: *"we are trying to avoid direct dependency, so can remove this — if anyone wants to use it, they can import the module."* Each consumer imports what it needs.
- **Local clients > shared clients across domains.** *"We should not use the existing `EntityDataServiceClient` in `entity-service`. Rather, we should create our own clients locally."* — Domain isolation is preferred over reuse.
- **Utils should be domain-specific, not generic `Utils` classes.** Generic `Utils` classes are flagged as antithetical to SRP. Prefer:
  - An interface like `*Constants` for constants
  - An injectable class with named methods (`buildKeyForSessionId(...)`) over scattered static helpers
- **`config-utils`, `config-proto-utils`, `audit-utils`, `modsecurity-utils`** are existing shared utility modules — prefer adding shared helpers there over duplicating across domain impls.
- **`UuidGenerator`** lives in `config-utils` — don't inline UUID generation.

## When to split a module

- **New auth/permission concerns** → consider an auth interceptor module.
- **A service consumed by multiple domains** → consider extracting a `*-client` module.
- **Local processing vs platform concerns** mix → split into `local-processing` modules (pattern is established).

## Settings.gradle.kts and registration

- New module names in `settings.gradle.kts` must match directory names exactly.
- New `-impl` modules must be registered in the right factory (`traceable-config-service-factory` or a domain factory) to be exposed via gRPC.

## Helm and deployment

- Some changes are configuration only and should be done via Helm values, not code (e.g. ignore lists, default rasp data types). Reviewers ask: *"shouldn't we drive this via config? something like `ignoreDefaultDataTypes {rasp = [...]}` in helm or some FF instead of baking into the code?"*

## How to apply during review

- **P3** if domain-specific code lands in the main service or a generic util.
- **P3** if a generic `*Utils` class is added for a single domain (suggest a typed/injectable alternative).
- **P3** if a module reuses another domain's client instead of creating a local one.
- **P0** if a new `-impl` module isn't registered in the appropriate factory (silent: service won't be exposed).
- **P2** if hardcoded config values could be Helm-driven.

**Why it matters for review:** Module boundaries protect each domain from cross-domain breakage. Misplaced code becomes coupled and hard to extract later.

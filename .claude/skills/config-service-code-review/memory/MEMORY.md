# Memory Index — config-service-code-review

Domain knowledge and reviewer priorities for `Traceableai/config-service` PR review. Built up across review sessions; refined as the codebase and team evolve.

## Domain knowledge (apply on every review)

- [Naming Conventions](domain_naming_conventions.md) — naming is treated as a P1, not a nit.
- [Proto Modeling](domain_proto_modeling.md) — oneof vs enum vs optional, defaults, deprecation, breaking changes, Create/Update/Get split, `buf.yml` registration.
- [Tenant Scoping & RequestContext](domain_tenant_scoping_request_context.md) — pass `RequestContext` explicitly; never put `tenant_id` in proto; use `ContextualKey` for caches.
- [gRPC Handler Pattern](domain_grpc_handler_pattern.md) — the canonical `try/catch + log(request, requestContext) + onError(e)` shape reviewers refer to as "our pattern".
- [Agent Compatibility](domain_agent_compatibility.md) — proto changes consumed by the agent; breaking changes require agent version gates.
- [Validation & Error Handling](domain_validation_and_error_handling.md) — gRPC status codes, no `Preconditions`, validator placement, no-throw on agent paths.
- [Caching Patterns](domain_caching_patterns.md) — `ContextualKey` keying, invalidation on upsert/delete, when not to cache.
- [Change-Event Cache Invalidation](domain_change_event_cache_invalidation.md) — caches over derived configs subscribe to change events; invalidate scoped, not blanket; long TTL is fine when listener exists.
- [Optional, Streams & Immutability](domain_optional_streams_immutability.md) — `.get()` is a red flag; prefer streams; `Collectors.toUnmodifiableList()`.
- [Module Organization](domain_module_organization.md) — when to split modules, utils placement, no direct dependencies between domains.
- [PR Scope Discipline](domain_pr_scope_discipline.md) — split big PRs, defer non-required functionality, one PR per concern.
- [Backwards Compatibility & Migration](domain_backwards_compat_migration.md) — migrations for existing stored configs, dual-write/dual-read patterns, default fallbacks for old data.
- [Submodule Discipline](domain_submodule_discipline.md) — accidental `hypertrace-config-service` pointer changes; CI enforces commits live on submodule's main.
- [Lombok & Guice Patterns](domain_lombok_guice_patterns.md) — `@Singleton` on services, `@Inject` constructor (no field injection), redundant `@Getter` on `@Value`.
- [Test Patterns](domain_test_patterns.md) — unit vs integration boundary, mock dependencies, no Lombok in tests, integration tests should not publish.
- [Helm/Configurability](domain_helm_configurability.md) — hardcoded values prompt "make it configurable via helm" — recurring reflex.
- [Default Configs & Default Behavior](domain_default_configs.md) — when to fall back to defaults, "comparing to default instance" code smell, default-on-error pitfalls.
- [Global Default Overrides](domain_global_default_overrides.md) — overlay tenant overrides on shipped global defaults; never copy-and-mutate.

## Reviewer priorities

- [Reviewer Priorities](reviewer_priorities.md) — what reviewers prioritize, how findings should be weighted, and the established delivery style.

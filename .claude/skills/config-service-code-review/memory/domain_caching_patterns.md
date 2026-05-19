---
name: caching-patterns
type: domain-knowledge
---

Caching is a high-attention area in this repo. Reviewers have repeatedly fixed the same mistakes.

## Cache-key rules

- **Use `ContextualKey<T>` as the key**, not raw tenant ID strings. Two flavors exist:
  - `buildUserContextualKey(...)` — looks at auth, use for user-scoped data
  - `buildInternalContextualKey(...)` — only tenant, use for ingestion / background paths
- **The cache key must be the most restrictive context that affects output.** If different users see different results, key by user; if different envs see different results, include env.
- **Don't return `null` from a Guava `CacheLoader`.** It throws `InvalidCacheLoadException`. Return an empty/default value, or wrap in `Optional`.

## Invalidation

- **Cache invalidation on upsert/delete must be explicit.** Stale reads after writes are the #1 cache bug here. Reviewers will check: when the upsert API is called, does the corresponding cache entry get invalidated?
- **Multi-replica cache invalidation is a known broader problem** — don't assume single-replica behavior.
- **DB write race**: caching in front of writes that contend on a unique key is dangerous; prefer validation on the template/spec level.

## When NOT to cache

- **One-shot lookups** (single ID, called once per request) — caching just adds invalidation complexity.
- **Provider-injected maps** are NOT caches — they're injected once at module construction. *"It's going to get invoked once per construction of a class that injects this map, which means once on service startup. So we're caching for the duration of the process lifecycle, not 1 day. To actually enforce a cache duration, you'd want to inject a class with a fetch/get method, rather than the map directly."*
- **Hot-path validation** (e.g. blocking rules) where the lookup itself is cheap — only cache the *validation result* (e.g. hash → already-validated), not the data.

## When to cache

- **Read-heavy, latency-sensitive paths** — the dedicated `*-caching-client` modules exist for this.
- **External lookups** (entity-service, etc.) — should use the same cache config style as similar lookups (e.g. service-id resolution).
- **Validated blob hashes** — cache `hash → validated=true` to avoid re-validating identical blobs.

## Cache implementation

- `CacheBuilder.newBuilder()` is the standard. Reviewers expect:
  - `.expireAfterWrite(...)` with a `Duration`, not a magic number
  - `.refreshAfterWrite(...)` for keys you want refreshed in background
  - `.recordStats()` if metrics are wanted (skipped intentionally for some caches)
  - Comment explaining the chosen TTL when it's not obvious
- **Don't use static instances of caches** unless you have a specific lifecycle reason. Inject via Guice with `@Singleton` — reviewers prefer this over static.

## How to apply during review

- **P0** if upsert/delete doesn't invalidate the corresponding cache entry.
- **P0** if cache key is missing a dimension that affects results (causes cross-user / cross-env data leak).
- **P1** if caching is added to a one-shot lookup path with no benefit.
- **P2** if `CacheLoader` returns null.
- **P2** if a "cache" is actually an injected provider-built map (not a real cache).
- **P3** if cache TTL is a magic number with no comment.

## See also

- [[change-event-cache-invalidation]] — recent recurring pattern: caches that hold derived configs should subscribe to config change events and invalidate scoped (not blanket).

**Why it matters for review:** Cache bugs are silent — they show up as "wrong data for some user sometimes" and are hard to repro. Specific patterns have accumulated from past incidents.

---
name: change-event-cache-invalidation
type: domain-knowledge
---

A specific cache pattern that reviewers consistently push for.

## The pattern

Caches that hold derived/composed config data should:

1. **Subscribe to config change events** for the config(s) they depend on, not just rely on TTL.
2. **Use a long TTL** (e.g. 1h) because the change-event listener handles staleness; a 5m TTL just creates load with no benefit. *"since we are using change event cache expiry should be large, keep it 1h"*, *"we don't need to update cache i every 5minute, we are any way listening to change event listener, so this duration can be high"*.
3. **Invalidate scoped, not blanket.** *"can we make this invalidation scoped? to tenant or env whatever is feasible... this might require changing the cache key"*. Don't `invalidateCacheForTenant` on every event — only when the specific config that affects the cache changed. *"we should not always invalidateCacheForTenant, it should be only if something for this config is changed"*.
4. **Filter the event** — only act when the change event is relevant to this cache's contents.

## When to apply

- A cache holds composed data derived from one or more configs that have their own change events
- A cache holds tenant-scoped or env-scoped data where some changes affect only a subset of cached entries

## When NOT to apply

- Cache holds data unrelated to a config that emits change events (no listener possible)
- TTL is intentionally short for other reasons (e.g. data freshness independent of config)

## How to apply during review

- **P0** if a cache reflects a config but doesn't subscribe to change events AND has a short TTL (will serve stale data in steady state).
- **P2** if change-event invalidation is wired but invalidates the entire cache instead of the affected scope.
- **P2** if cache TTL is short (≤ 5m) AND there's already a change-event listener (redundant load).
- **P3** if a change-event listener invalidates without checking whether the event is relevant.

**Why it matters for review:** Cache+change-event patterns are the established way to keep derived configs fresh in this repo. Skipping the event subscription forces short TTLs (latency for users) or long staleness (wrong behavior). The skill should call this out explicitly.

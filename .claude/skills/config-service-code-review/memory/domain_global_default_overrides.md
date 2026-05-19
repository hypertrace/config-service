---
name: global-default-overrides
type: domain-knowledge
---

Specific design pattern for "global default config that tenants can override". This came up explicitly in the `ip-resolution-strategy-config-service` PR review and was acknowledged as the established pattern.

## The pattern

Global defaults are shipped in a resource file (e.g. `default-*-configs.conf`) inside the `-impl` module. When a tenant wants to disable or modify a default:

- **Don't mutate the shipped file.**
- **Don't copy the global into tenant storage and edit it** — that breaks future global default updates from reaching the tenant.
- **Layer a tenant-stored override with the same id on top of the global.** Read path merges global + tenant override; tenant override wins for fields it sets.

Example (from review):
```
foo (global default, disabled=false)         # shipped in resource file
foo (tenant override, disabled=true)         # stored per-tenant
```
On read for that tenant: the override's `disabled=true` wins; the rest of the config still comes from the global.

## Why this matters

- If the team updates the global default (e.g. fixes a bug, adds a new field), every tenant gets the fix automatically — except for the specific override fields the tenant set.
- If the team copied the global into tenant storage on first override, every tenant would be stuck with the snapshot at override time. Operationally bad.
- Reviewers explicitly flagged the copy-and-mutate model: *"Seems like our current design is bound to run into bugs"* (when a reviewer mistakenly thought that was the model).

## Three sets of defaults (existing pattern)

The repo already supports three categories of defaults — this overlay model fits all three:
1. **Defaults user CAN'T edit** — shipped, no override allowed
2. **Defaults user CAN edit** — shipped, tenant overlay supported
3. **User-created** — purely tenant-stored, no global

The override pattern is for category 2.

## How to apply during review

- **P0** if a new "default config" feature copies-and-mutates instead of overlaying — flag the design explicitly with the alternative.
- **P0** if the read path doesn't merge global + tenant override (will return wrong results when tenants override).
- **P2** if there's no validation preventing tenant overrides from changing immutable fields (e.g. `id`).
- **P3** if the override model isn't documented in code (next reader will guess wrong).

**Why it matters for review:** Default config layering is the kind of design that's hard to fix once shipped — once tenants have copies in storage, migrating to overlays is painful. Get it right on PR.

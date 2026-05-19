---
name: backwards-compat-migration
type: domain-knowledge
---

Configs are persisted documents. Existing stored configs must remain readable after schema changes. Reviewers check this carefully.

## Rules

- **On read paths**: when adding a new field, fill it from the deprecated/old field if absent — until all writers migrate. *"where is the backward compatibility code to fill the new fields from the deprecated ones in fetch call?"*
- **On write paths**: when removing/deprecating a field, dual-write for at least one release so old readers still see data.
- **Default-on-empty for legacy data**: if a previously-required field is now optional (or vice versa), the impl needs explicit fallback. *"Also, in impl, need code for backward compatibility — if the list is empty, default to ALERT."*
- **Don't ship a breaking config schema change without a migration**. The MongoDB store doesn't auto-migrate; old documents will fail to deserialize unless the proto change is additive.
- **Existing tenant configs**: changes that affect default behavior may silently flip behavior for existing tenants. Verify: are existing tenants affected? Is there a Helm-values audit needed? *"Since we haven't migrated the existing configs, this would disable local API naming for everyone."*

## Migration patterns endorsed

- **Versioned namespace** for major schema changes — each version has its own subtree, no migration needed because old/new coexist. *"That would be part of that version config in its own namespace — this denotes migration to this version (v2). Whenever there is a v3, it will have its own set of configs in its own namespace."*
- **Migration config services** (e.g. `DetectionExclusionMigrationConfig`) — separate config service whose job is to drive the migration. New PRs should use this flow rather than bulk-upserting all rules at once.
- **FF-gated rollouts**: new behavior gated by feature flag + min TPA version, old behavior is the default until the gate flips.
- **Don't migrate by upserting all rules.** *"We should not be upserting all rules. Let's not do this here — add support in `DetectionExclusionMigrationConfig` and use that flow instead."*

## Agent-side compatibility

- An old agent talking to the new API with the FF enabled must keep working as before. Don't require the agent to know about new fields unless gated.
- Adding a new enum value to an agent-consumed enum: old agents will fall into the `default` case in their switch — make sure that case is sensible (skip / warn / default action).

## Migration-as-code-debt

- Reviewers push back when migration code starts to feel permanent. *"We're dancing around the main point though — do we even need the migration? who really cares that some historical queries are missing the email?"* — they prefer dropping migrations when the cost > benefit.

## How to apply during review

- **P0** if a proto change makes existing stored configs unreadable.
- **P0** if a behavior change silently flips defaults for existing tenants without a Helm-values check or migration.
- **P0** if a new field doesn't have read-side fallback from the deprecated field it replaces.
- **P2** if migration is done by bulk upsert instead of via the established migration-config-service flow.
- **P3** if migration code is added without a removal plan/timeline.

**Why it matters for review:** Stored configs in MongoDB are the source of truth; breaking their readability is a P0 production incident waiting to happen.

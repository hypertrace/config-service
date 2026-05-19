---
name: naming-conventions
type: domain-knowledge
---

Naming is **the most-discussed review topic** in this repo by a wide margin. Reviewers treat naming as a correctness/clarity issue, not a nit. They will block merges over it.

## Concrete patterns reviewers consistently push back on

- **Generic / non-domain names**: `association`, `runner`, `data`, `entity`, `info`, `details`, `Utils` — reviewers immediately ask "what does this mean?" and propose domain-specific replacements (e.g. `RateLimitedEntity` instead of `Association`, `BlockingPolicyData` instead of `Data`).
- **Names that don't match behavior**: methods named `fetch*` that don't actually return data, `validate*` that returns instead of throws, `update*IfApplicable` that sometimes creates, methods returning `true` when something *doesn't* match.
- **Inconsistency between proto field name and what it carries**: e.g. a field named `key` and `attributeKey` holding the same value, `ApiNamingConfigInfo` (drop `Info`), `BlockingDetails` vs `BlockingDetailsCondition`.
- **Old vs new product naming**: legacy names that no longer match the product UI (e.g. "tags" was renamed to "labels"; reviewers will flag `tag_id` and demand `label_id`).
- **Plural/singular mismatch**: `attribute` vs `attributes` (reviewers flag plural form when field is repeated).
- **Hierarchy encoded in name**: scopes named to imply hierarchy when product wants flat — reviewers prefer flexibility, push back on names that lock in nesting.
- **Units missing from name**: timeouts/durations as bare numbers without unit in the name. Either put unit in the name (`*_seconds`, `*_minutes`) or use a `Duration` type.
- **Confusing positives/negatives**: methods or flags whose name reads opposite of what they do (e.g. `isUnsetScope` returning a boolean — naming alone makes call sites unreadable).
- **Reusing names across messages with different semantics**: same field name across messages where the behavior differs — reviewers ask to disambiguate.

## Reviewer phrasing to watch for

- "Naming here feels off"
- "Better name could just be `X`"
- "Can we rename it to `X`?"
- "What's a `<name>`? Can you share context?"
- "It's not really `<X>` specific, it's `<Y>`"
- "We shouldn't need a comment to convey the meaning of a name"

## When generating findings

If a new proto message/field, a new method, or a new class uses any of the patterns above, raise it as **P3 (code design)** at minimum — and **P2** if the name is actively misleading about behavior. Always propose a concrete alternative name, don't just flag.

**Why it matters for review:** Naming defects accumulate and outlive the PR. Once a generic name ships in a proto, it's a breaking change to fix. Reviewers treat this as a P1-class concern; the skill must mirror that.

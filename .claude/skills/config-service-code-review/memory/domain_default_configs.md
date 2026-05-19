---
name: default-configs
type: domain-knowledge
---

"Default" is one of the most-discussed concepts in this repo. Reviewers have specific opinions on when defaults apply.

## Three distinct meanings of "default" in this repo

1. **Default config returned when no tenant config exists** — fall back to a system default. Read path returns the default as if the tenant had it.
2. **Default rule set shipped with the product** — rules the agent uses out of the box. Stored in code/resources, not per-tenant in DB.
3. **Default field value (proto3 zero)** — what an absent field deserializes to.

Reviewers are pedantic about which one is meant. Confusing them is a common bug source.

## Rules

- **"Default" should mean "absence of config", not "a configured mode named default".** *"I think we should drop the term 'default' protection mode from the APIs, unless there is a use case around it. Default usually means if the config is not present, return the default."*
- **Default config on read**: catch `NOT_FOUND` from store and return default. Don't pre-build a default and return it from the no-filter path. *"This is the default so not effectively changing right? Just confirming we don't need to touch the build job's consumption of this cache."*
- **Default rules are not editable by users in some categories, partially editable in others, fully editable in others.** Three sets exist; design must support this layering.
- **Don't let the source of a default rule become "default" via update API**. Add validation in create/update path that source can't be changed to default.
- **3 sets of defaults**: (1) defaults user cannot edit, (2) defaults user can edit, (3) user-created. Design must accommodate all three without code changes when a rule moves between sets.

## Default-on-error

- **Don't default on error inside cache loaders** — the cache will store the default permanently. Let the loader throw, cache will reject, next request retries. Default on error happens in the *consumer* of the cache, not the loader.
- **Default on read = OK**, default on write = usually wrong. If a user explicitly clears a value, that's not "absence" — it's an explicit clear, which is meaningful.

## How to apply during review

- **P0** if "default" semantics are confused (e.g. an API exposes a "default mode" config that should just be absence-of-config).
- **P2** if default-on-error happens inside a cache loader (will poison the cache).
- **P2** if create/update doesn't validate that source/origin field can't be changed to "default".
- **P3** if `default` keyword in a switch is missing (need it for new enum values to fail-fast or warn).

## "Comparing to default instance" code smell

A recent reviewer comment establishes a specific anti-pattern to flag:

> *"Comparing to default instance is a code smell — it means that either someone is explicitly sending default instance, or we're not checking presence when we access spanFilter from its parent object. Either way, we're hiding an upstream bug. I'd suggest recursing up to find it, but if you want to be defensive here (in general we can't always control clients, but since this is internal we should be able to), instead you could add a case statement below for the unset case."*

When code does `if (config.equals(SomeMessage.getDefaultInstance()))`:
- **P2** — likely hiding an upstream bug. The right fix is `parent.hasConfig()` check at the access site, OR a `oneof` `unset` case.
- Don't accept "but the parent might forget to set it" — the fix is to make the parent's contract explicit, not to silently treat default-instance as "missing".

## See also

- [[global-default-overrides]] — three-set defaults model with tenant overlay layering on top of shipped global defaults.

**Why it matters for review:** "Default" overloading causes silent product bugs — users see the default config but can't tell whether it's "system default", "their saved config", or "their cleared config". The reviewers untangle this constantly.

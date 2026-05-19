# Reviewer Priorities — config-service

What reviewers in this repo consistently care about, how they weigh findings, and how they expect feedback to be delivered. Update in place after each review session.

## Reviewer style at a glance

- **Short and imperative.** Most feedback is a question ("Why this?", "Should be Get request"), a single-line direction ("mark singleton", "rename to X"), or a code-block suggestion. Match this register in skill output — paragraphs are not the norm.
- **Inline on specific lines**, not summary comments. Findings should be tied to exact files and lines.
- **Concrete alternatives included** — "rename to X", "use Y instead", with code snippets. Don't just identify a problem; propose the fix.

---

## Reviewer signatures (composite)

These are the recurring lenses reviewers apply. Findings should match these patterns regardless of who is on a given PR.

### Style/idiom enforcement
- `Optional.get()` is a "red flag", prefer `stream/map/foreach` over loops, prefer `orElseThrow()` over `if/throw`, `Collectors.toUnmodifiableList()` everywhere.
- **Architectural pushback**: questions whether new modules/abstractions are warranted ("why not drop the interface?", "why not pass a channel instead of a stub?", "this defeats the purpose of the abstraction").
- **Domain modeling sharpness**: pushes for precise data structures over loose ones (e.g. "describe the nodes to remove from a tree" vs "describe paths"; "this is a more powerful enum with heterogeneous parameters" re. `oneof`).
- **Naming as a P1**: "we shouldn't need a comment to convey the meaning of a name". Will request specific renames with concrete suggestions.
- **Pragmatic about scale**: "if the implementation in different repos doesn't share data, we don't need consistency" — willing to drop cross-system consistency when it adds complexity.
- **Style of delivery**: opens with "nit:" for stylistic, "I would" / "prefer" for opinionated, direct push-back for design issues. Often follows up with a concrete code snippet.

### Functional correctness + scope discipline
- **Functional correctness**: pushes hard on edge cases, default values, validation completeness.
- **PR scope discipline**: regularly asks for changes to be split into separate PRs ("only proto changes here, impl in next PR").
- **Backwards compatibility**: routinely asks "where is the BC code?", "how does this work for existing tenants?", "are existing configs migrated?".
- **Move it / rename it**: short asks ("nit: move this to X", "nit: rename to Y"). Concrete and direct.
- **Hardcoded → Helm**: a strong reflex to ask why something isn't in `application.conf` / Helm.
- **Style of delivery**: short, often imperative ("plz add", "let's not", "we should"). Frequent use of "nit:" prefix.

### Test discipline + tenant scoping
- **Test discipline**: "unit test should mock dependencies", "use Guice for bigger tests", clear about what counts as a unit vs integration test.
- **Tenant scoping**: "we should never have tenant ids in the protos" is a recurring rule.
- **Validator placement**: prefers a single validator per service over scattered checks.
- **Module reuse vs duplication**: leans toward local clients over cross-module shared clients ("create our own clients locally").
- **Style of delivery**: explanatory ("Why? The test can either chose to..."), often educational. Will flag a pattern across multiple files.

### Lombok/Guice cleanups + naming consistency
- **Lombok/Guice cleanups**: "@Singleton missing", "@Value already implies private final", "use lombok with @Value".
- **Naming consistency**: pushes for matching names between proto and impl ("rename to BlockingPolicyData").
- **Short, direct nits** ("Why this change?", "nit - Unused", "Nit -> Maybe have a single...").
- **Performance/efficiency questions**: "Why are we doing this?", "Why are we using such an older version?".
- **Style of delivery**: terse, question-form, often prefixed `nit -` or `nit ->`.

---

## Cross-reviewer consistent priorities (highest weight in skill output)

These are the things reviewers consistently raise:

1. **Naming** — call out misleading or generic names with a concrete alternative
2. **Proto modeling** — `oneof` vs enum, defaults, breaking changes, agent compatibility, Create/Update/Get split
3. **Tenant scoping / RequestContext** — never in protos, always passed explicitly
4. **Validation correctness** — right gRPC status codes, no `Preconditions`, single validator
5. **Caching invalidation** — upsert/delete must invalidate; subscribe to change events for derived configs
6. **Scope discipline** — split big PRs, defer non-required functionality
7. **Helm/configurability** — hardcoded values should be Helm-driven
8. **Backwards compatibility** — read-side fallbacks, migration plan for breaking changes

## Net new themes worth tracking

Patterns that have become more prominent recently:

- **Change-event listeners on caches** — derived caches should listen to upstream config-change events; long TTL is fine when the listener exists; invalidation must be scoped, not blanket.
- **Submodule-pointer hygiene** — accidental `hypertrace-config-service` updates are a regular issue; CI enforces commits live on submodule main.
- **Create vs Get vs Update message split** — `Create*Request` has no id; `Get*Request` for reads (not creative verbs); split `*Config` into id-bearing wrapper + `*ConfigData`.
- **Mark `@Singleton` explicitly** — short asks like "mark singleton" / "Converters can be singleton" — reviewers don't trust that the default is right.
- **`buf.yml` registration for new modules** — proto compatibility validation only runs for registered modules.
- **"Comparing to default instance is a code smell"** — should check `has*()` on the parent or use a `oneof unset` case.
- **"Defaults in config class, not test files"** — when adding a new field, set the default in the config class so tests don't all need to change.
- **"Log full requestContext, not just tenant id"** — the full RC is needed for production debugging.
- **Standard gRPC handler shape is "our pattern"** — single broad try/catch, log both `request` and `requestContext`, `onError(e)`.

---

## Cross-reviewer consistent deprioritizations

- **Test count for Lombok-generated code** — explicitly skipped
- **Stylistic uniformity across unrelated repos** — reviewers reject this
- **Pre-existing issues in the file** — flagged with "pre-existing, but..." but not blocking

---

## How findings are typically delivered

- **Inline on specific lines**, not summary comments. Reviewers expect findings tied to exact lines.
- **Concrete alternatives included** — "rename to X", "use Y instead", with code snippets.
- **`nit:` prefix** for low-priority stylistic items. Use this prefix in skill output for P4 findings.
- **Question form** ("why do we need this?", "why not X?") is heavily used for design challenges. Mirror this for P3 findings — it invites the author to justify rather than asserting they're wrong.
- **"I would..." / "prefer..."** for opinionated-but-not-blocking items.
- **Short** — most feedback is one or two sentences. Don't write a paragraph when a sentence will do.

---

## Calibration notes for the skill

- **Don't over-flag**: small drive-by changes don't need a P4 nit on every line. Reviewers reserve detailed feedback for areas where the design matters.
- **Naming is NOT a P4** in this repo. Treat it as P3 minimum, P2 if misleading.
- **`Optional.get()` and `for + continue`** show up often enough that flagging them is high-confidence — but mark as P3, in line with reviewer treatment.
- **Always include a concrete alternative or rename suggestion** — that's the established style.
- **Question form for P3 design comments** ("Why do we need X?") is more effective than assertion form.

## High-confidence patterns

Patterns where reviewer feedback reliably converts to code changes — flag these with confidence:

- **Mark `@Singleton`** — short "mark singleton" / "Converters can be singleton" asks → consistently fixed.
- **Rename to a more specific name** — explicit rename suggestions are followed almost always.
- **Add `requestContext` to logs** — small ask, easy fix, consistently done.
- **Mark message/field as deprecated** with comment — reliably done when flagged.
- **Move it / inline it / dedupe it** — refactor-in-place asks are followed.
- **Remove the unused** (annotation, import, code path) — easy to do, reliably fixed.
- **"Don't compare to default instance — use `has*()`"** — addressed when raised.
- **"Add tests for boundary values"** — addressed when raised.
- **"Source/sink config should be in `application.conf`"** — pattern of "this should be Helm/conf-driven" reliably converts.

When generating findings, **lean into these patterns**. Lower-confidence items (deep design suggestions, big refactors) should be phrased as questions to invite discussion rather than as demands.

## Lower-confidence patterns

Patterns where authors often push back successfully:

- Speculative "do we even need this?" challenges on already-built features → author defends, reviewer accepts.
- Type-precision asks ("should this be float, not int?") on UI-driven configs → author defers to product decisions.
- "Use a `oneof` here for future extensibility" when the author has a clear use case for the simpler shape → reviewer accepts.

When uncertain whether a finding will land, prefer question form ("Should this be X?") over assertion ("Change this to X").

---

## Updates after future sessions

After each review session, append observations about:
- Findings the reviewer chose to post on the PR (keep doing)
- Findings the reviewer dropped or marked as not relevant (deprioritize)
- New patterns that emerged from the conversation
- Refinements to delivery style preferences

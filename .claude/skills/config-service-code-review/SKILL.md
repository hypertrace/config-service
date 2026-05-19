---
name: config-service-code-review
description: Domain-aware code review for config-service PRs, with memory-backed continuous learning
globs:
  - "**/*.proto"
  - "**/*-config-service-api/**/*.java"
  - "**/*-config-service-impl/**/*.java"
  - "**/*-config-service-*-client/**/*.java"
  - "traceable-config-service-factory/**/*.java"
  - "buf.yaml"
---

# Config Service Code Review

Review a PR in `Traceableai/config-service` with awareness of the framework's conventions: gRPC service patterns, proto compatibility, Guice wiring, tenant scoping, store/persistence layer, and factory registration. Accumulates domain knowledge across reviews via `memory/`.

## When to Use

- **Any request to review a PR in `Traceableai/config-service`** — use this skill by default for all PR reviews in this repo (e.g. `/config-service-code-review <PR>` or "review PR #1234")
- A PR touches `*-config-service-api/`, `*-config-service-impl/`, client modules, or `traceable-config-service-factory/`
- Proto changes need wire-compatibility review

## When NOT to Use

- Generic code review with no config-service-specific concerns — the standard `/review` skill is fine

## Required Information

Before starting, the user should provide:

- **PR URL or PR number** for the PR to review (e.g. `https://github.com/Traceableai/config-service/pull/1234` or `1234`)

---

## Step 1: Load accumulated memory

Read `.claude/skills/config-service-code-review/memory/MEMORY.md` (project-local) to get the index of all past learnings.
Then read every file listed in that index. Internalize them as **domain context** — system architecture, intended behaviors, and invariants that the reviewer has explained across past reviews. Use this context primarily to validate P0 functionality findings: does the code actually do what the system requires?

If MEMORY.md does not exist yet, proceed without it.

---

## Step 2: Fetch PR details and check out the branch

**2a. PR metadata + diff**

Use `gh pr view <PR> --repo Traceableai/config-service` and `gh pr diff <PR> --repo Traceableai/config-service --name-only` to see what changed.

Then fetch the patches for non-lockfile files:
```
gh api "repos/Traceableai/config-service/pulls/<number>/files?per_page=100" \
  --jq '.[] | select(.filename | endswith(".java") or endswith(".proto") or endswith(".kts") or endswith(".kt")) | {filename, additions, deletions, patch}'
```

Skip `gradle.lockfile`, `settings.gradle.kts` version bumps, and generated proto code — automated dependency updates aren't worth reviewing unless they introduce an unexpected new dependency.

**2b. Check out the PR branch in a worktree so full files can be read**

The diff alone is not enough for this repo's review style. To read whole files, neighboring files, callers, and parallel implementations:

```
gh pr checkout <number> --repo Traceableai/config-service --recurse-submodules
```

Or, to keep `main` clean, use a worktree:
```
git fetch origin pull/<number>/head:pr-<number>
git worktree add /tmp/pr-<number> pr-<number>
```

Then operate in that working tree for the rest of the review.

**2c. Read full context before forming findings — do NOT review from the diff alone**

For every PR, before drafting findings, gather the following set of files. The list of *what* to read is the same as before; the change is *how*: build the list first, then dispatch it in parallel.

**Files to read:**

- **Every file touched in the PR — fully**, not just the patched hunks. The diff hides surrounding state (existing imports, neighboring methods, class-level annotations like `@Singleton`, the rest of an enum or constants block).
- **The immediate neighbors of any new class/file** — the directory listing of the file's package, plus the 1–2 most semantically related files (e.g. an existing `PiiRule…Converter` when reviewing a new `DatatypeRule…Converter`).
- **The file declaring any helper whose signature changed**, plus its other callers, found via `grep`.
- **Every caller of any renamed symbol** — `grep -rn "OldClassName\|oldMethodName" .` across the whole repo, including the `hypertrace-config-service` submodule. Stale callers are a frequent miss.
- **The parallel implementation** — when a PR adds rule type N, read the existing rule-type converters/validators end-to-end so you can judge whether the new code duplicates structure that should be shared, and whether naming/conventions match.
- **The constants/enums block being extended** — read the whole file, not the inserted lines, to compare casing/wording against neighbors.
- **The agent-side consumer** if a new enum value, new field, or new RPC was added — search for switch statements over the changed enum (`grep -rn "<EnumType>" .`) and confirm the new arm is handled or that there's an explicit fallback.
- **Helm chart files** under `helm/` if any tunable, threshold, or feature flag was hardcoded — confirm whether it should be Helm-driven.
- **Tests for the parallel implementation** — read the existing test for the analogous rule type so you can judge whether the new tests cover the same negative cases (disabled, missing fields, edge values).

**Dispatch reads in parallel.** Serial reads are the dominant latency cost of this procedure — do not read files one at a time. Two patterns:

1. **Multi-Read in one message (preferred for ≤ ~10 files).** After Step 2a gives you the changed-file list and the neighbor/caller list from greps, issue all `Read` tool calls in a **single assistant message** with multiple tool-use blocks. They will execute concurrently. Do not interleave reads with analysis text — collect first, analyze after.

2. **Subagent dispatch (preferred for > ~10 files, or when reads can be grouped by topic).** Dispatch a single `Agent` call with `subagent_type=Explore`, giving it the explicit list of files to read and a focused question (e.g. "read these 12 files and report: any callers of `OldClassName` still using the old name; whether `@Singleton` is on every new converter; whether the new enum value is handled in every switch over `AiAppCustomRuleType`"). The subagent reads in parallel inside its own context, returning a summary that doesn't bloat your main context. Use multiple `Agent` calls in one message when the topics are independent (e.g. one for proto/wire-compat, one for caller analysis, one for test coverage).

Greps that feed the read list (`grep -rn "OldClassName" .`, package directory listings, `grep -rn "<EnumType>" .`) should also be batched in a single message — they are independent.

**Rule of thumb:** every clarifying question of the form *"could you paste X?"* or *"does Y exist?"* is a question you should answer yourself by reading code before asking the author. Reserve clarifying questions for product intent, agent-version constraints, deployment plans, and other things that genuinely live outside the repo.

**2d. Proto compatibility**

If `.proto` files changed, run `buf breaking --against .git#branch=main` against the diff to flag potential wire-incompatible changes (field renumbering, removed fields without `reserved`, type changes, enum value removal, and **`FIELD_SAME_ONEOF` — moving an existing standalone field into a `oneof`**).

---

## Step 3: Summarize the PR and ask clarifying questions

**First**, write a plain-English summary of the PR covering:
- What problem it solves / what feature it adds (based on the PR description and Jira ticket title)
- Which modules changed (`-api`, `-impl`, client, utility) and what each change does at a high level
- Any notable patterns, risks, or open questions you spotted just from reading the diff (proto changes, Guice wiring changes, store/persistence layer changes, factory registration order)

**Then**, ask all clarifying questions in a single numbered list before doing any review. Do not hold back — the more context you get, the better the review. Good questions to consider (use these as prompts, not an exhaustive script):

- What is the intended end-to-end behavior of this config service / RPC? Who is the upstream consumer (UI, agent, another backend service)?
- Is this a new config service or an extension of an existing one? If new, why doesn't an existing service cover this domain?
- Are proto changes additive only, or are there breaking changes? If breaking, has every consumer been identified and updated?
- Are new fields meant to be required or optional? Is default behavior for missing fields documented?
- Does this config interact with the agent? Does the agent need a corresponding code change to consume new fields?
- Is there a migration story for existing stored configs? Are old documents in the datastore still readable after the change?
- Are tenant-scoping and authorization handled correctly for new RPCs (RequestContext usage, tenant ID extraction)?
- Are there integration tests needed (MongoDB, gRPC) and are they present?
- Is there a Helm chart / deployment change needed alongside this PR, or does it stand alone?
- Are there known limitations or follow-up tickets already filed?

**Only ask things that genuinely require the author's product/business context.** Do not ask questions you can answer by reading the repo (existence of a class, casing of neighboring constants, whether a rename has stale callers, what an existing converter looks like). Do those reads in Step 2c.

**Wait for the reviewer's answers before proceeding to the review.** Do not guess — stop here and present only the summary and questions.

---

## Step 4: Domain-specific review

After receiving answers, review using the checklist below, plus any patterns loaded from memory in Step 1. Use the reviewer's answers to calibrate which findings are real issues vs. intentional decisions.

> **Cross-cutting priorities (highest weight, raised by all top reviewers):**
> 1. **Naming** — generic / misleading / behavior-misaligned names. The single most-commented topic.
> 2. **Proto modeling** — `oneof` vs enum vs optional, defaults, breaking changes, agent compatibility.
> 3. **Tenant scoping** — `RequestContext` passed explicitly, never `tenant_id` in protos, gRPC stub calls wrapped in context.
> 4. **Validation** — gRPC status codes (no `Preconditions`), single validator per service, validation logs at WARN not ERROR.
> 5. **Caching** — invalidate on upsert/delete, `ContextualKey` keying, never return null from `CacheLoader`.
> 6. **PR scope** — split big PRs (proto-only vs impl, migration as its own PR).
> 7. **Helm/configurability** — hardcoded thresholds/lists/timeouts should be Helm-driven.
> 8. **Backwards compatibility** — read-side fallback from deprecated fields; existing stored configs remain readable.
> 9. **"Default" semantics** — distinguish "absence of config" from "a configured mode named default".

### Proto definitions (`-api/src/main/proto/`)
- **Breaking changes**: field number reuse, removed fields without `reserved`, renamed fields/enum values, changed types — all break wire compatibility. Flag and require justification.
- **Field numbering**: new fields use the next available number; reserved numbers respected.
- **Enum hygiene**: new enums have a `*_UNSPECIFIED = 0` default; new enum values are appended, not inserted.
- **Optional vs. required semantics**: proto3 has no `required`, so callers may omit fields. Is the impl tolerant of missing fields, or does it NPE?
- **Naming conventions**: snake_case fields, PascalCase messages — match neighboring protos.
- **Service RPC naming**: matches existing `Get/Upsert/Delete/List` patterns where applicable.
- **Buf lint clean**: no warnings introduced.

### gRPC service implementation (`-impl`)
- **Extends generated base class** (e.g., `*ConfigServiceImplBase`) and overrides the right method signatures.
- **RequestContext / tenant scoping**: every RPC extracts tenant ID via `RequestContext.CURRENT.get()` and applies it to store reads/writes. Missing tenant scoping = cross-tenant data leak.
- **Authorization**: uses the standard authorization patterns from the generic config-service framework. Don't bypass without justification.
- **`onError` vs. `onCompleted`**: errors are reported via `responseObserver.onError(Status.X.asRuntimeException())` with appropriate codes (INVALID_ARGUMENT for bad input, NOT_FOUND for missing, INTERNAL for unexpected). Never let exceptions propagate uncaught.
- **Input validation**: required proto fields and IDs are validated before store calls. Empty strings, null lists, malformed IDs caught early with `INVALID_ARGUMENT`.
- **Idempotency**: upsert operations behave correctly when the same request is replayed. Delete operations don't fail when target is already absent (or do, with a clear contract).

### Guice modules (`*Module extends AbstractModule`)
- **Constructor injection**: `@Inject` on constructor, `@Singleton` where appropriate. No field injection.
- **Bindings**: services bound to their gRPC base class so they're picked up by the service registry. New modules added to the main `*ConfigServiceFactory` / `traceable-config-service-factory` if they should be deployed.
- **Dependency wiring**: new dependencies (stores, clients) provided by parent modules or `@Provides` methods. No `new` of injectable types inside modules.
- **Module composition**: `install(new OtherModule())` order doesn't introduce duplicate bindings. If the same key is bound twice, Guice fails at startup.

### Store / persistence layer
- **Tenant isolation**: store keys / queries always include tenant ID. Cross-tenant queries flagged.
- **Document schema migration**: changes to stored proto shape are forward/backward compatible, or there's a documented migration. Old documents must still parse.
- **Indexing implications**: new query patterns that scan large collections without indexes will degrade as tenants grow.
- **Optimistic concurrency**: if the framework supports versioned writes, new code uses them.

### Factory registration / service wiring
- **`traceable-config-service-factory` / main service**: new services are registered in the right factory and exposed via the gRPC server bootstrap.
- **Module install order**: matches existing pattern. Order matters when modules contribute to the same multi-binder.
- **`settings.gradle.kts`**: new modules are included; module names match directory names.

### Client modules (`*-client`)
- **Caching client correctness**: cache invalidation on upsert/delete is wired. Stale reads after writes are a common bug.
- **Retry / timeout policy**: matches existing client patterns. Don't introduce new timeout values without rationale.
- **Generic client vs. caching client**: chosen for the right reason (caching only for read-heavy, latency-sensitive paths).

### Logging
- **`@Slf4j` present but no `log.*` calls** → dead annotation, remove it.
- **Log on failure paths**, especially silent ones (empty results, missing tenant, validation rejections).
- **`log.isDebugEnabled()` guards** around `log.debug()` in hot paths (per repo style guide).
- **No PII in logs**: tenant IDs are okay; raw config payloads with sensitive data (API keys, tokens) are not.

### Testing
- **Unit tests** for every new service implementation, validator, and converter. Mock store and external clients; use real protos.
- **Integration tests** in `src/integrationTest/java` when MongoDB or full gRPC behavior is exercised.
- **Test both success and error paths**: invalid input → INVALID_ARGUMENT, missing → NOT_FOUND, unexpected → INTERNAL.
- **Tenant scoping verified in tests**: a test with tenant A's RequestContext should not see tenant B's data.
- **Verify exact response fields**, not just "no exception thrown".
- **Do not test Lombok-generated code** (getters/setters/builders).

### Build / dependencies
- **New dependencies**: are they already in the Traceable BOM version catalog? If not, OWASP dependency check (CVSS ≥ 7.0 fails) must be considered.
- **`owasp-suppressions.xml`** changes: justified and scoped, not blanket suppressions.
- **Submodule (`hypertrace-config-service`)**: if updated, points to a commit on submodule's `main` (CI enforces this).
- **Helm chart**: changes match service/module changes (new ports, env vars, config maps).

### General
- Magic numbers or strings without comments explaining their origin or rationale
- Unused imports, annotations, or private methods
- FQCNs used instead of imports (per repo style guide)
- Single-character lambda parameter names
- Constants whose names don't convey their semantics

---

## Step 5: Present the review

Group findings by priority level:

**[P0] Functionality** — The happy path must work and all functional scenarios (including negative cases) must be handled. Fix before merge.
Examples: cross-tenant data leak from missing RequestContext scoping, breaking proto change without consumer updates, missing factory registration so new service isn't actually exposed, unhandled exception path that crashes the gRPC handler, store schema change that breaks reads of existing documents.

**[P1] Performance** — Code that causes measurable performance degradation: queries without supporting indexes, N+1 store calls per RPC, cache invalidation gaps causing stampedes, unbounded `List` RPCs without pagination, excessive proto allocations in hot paths.

**[P2] Bugs / Corner cases / Bad input** — Issues a QE would catch during testing: missing input validation (empty IDs, null lists), wrong gRPC status codes, idempotency violations, silent failure paths without logging, missing edge case tests, race conditions on concurrent upserts.

**[P3] Code design** — Class structure, method decomposition, abstraction quality, use of Java design patterns and the framework's conventions. **Naming defects (generic, misleading, or behavior-misaligned names) belong here at minimum** — naming is treated as a P3 in this repo, not a nit. Bump to **P2** if the name is actively misleading about behavior.
Examples: bypassing the generic config-service framework instead of extending it, duplicating logic that already exists in a base class, Guice bindings that should be in a shared module, converters/validators inlined in the service impl instead of extracted, generic class/field names like `Info`/`Data`/`Utils`/`Association`/`Runner`, `Optional.get()` usage, `for + continue` antipattern, mutable collections in responses, missing `@Singleton` on services, redundant `@Getter` on `@Value`, `@Slf4j` without `log.*` calls, hardcoded values that should be Helm-driven.

**[P4] Everything else** — Pure style nits with no correctness impact.
Examples: unused imports or annotations, single-character lambda parameter names, redundant `.build()` calls, indentation, comment formatting.

Also call out **positives** — good things worth reinforcing.

For each finding, cite the exact file and relevant code snippet, and **propose a concrete alternative** (rename, code snippet, or specific refactor) — that's the established style in this repo.

### Delivery style — match the reviewers' established patterns

- **Inline-on-line, not summary.** Tie every finding to a specific file and line.
- **Question form for P3 design challenges** — "Why do we need this?", "Why not X?". Invites justification rather than asserting wrong.
- **`nit:` prefix for P4 only.** Do not use `nit:` for P3 naming/idiom items.
- **Short.** Most reviewer comments are 50–200 chars. One sentence + one suggestion is the standard shape.
- **Concrete alternative included.** Every rename suggestion includes the proposed name. Every "use X instead" includes the snippet or method name.

---

## Step 6: Reflect and update memory

Memory serves one long-term goal: close the gap between what you can see in the code and what the reviewer knows from business, product, and architectural context. The target is to eventually review PRs as well as or better than the reviewer. There are two types of memory to maintain.

---

### Memory type 1 — Domain knowledge (`domain_*.md`)

Save what came from the reviewer's answers: system architecture, end-to-end flows, invariants, intentional design decisions that look like bugs from code alone, and constraints that govern future design. These help validate P0 functionality — whether the code does what the system actually requires.

Do NOT save: code antipatterns, style issues, test coverage gaps (derivable from code), PR-specific details, or anything already in memory.

Format:
```
---
name: <short descriptive name>
type: domain-knowledge | false-positive
---

<2-3 sentence description>

**Why it matters for review:** <how this helps validate P0 functionality>
```

---

### Memory type 2 — Reviewer priorities and review philosophy (`reviewer_priorities.md`, a single evolving file)

After every PR review session, update `reviewer_priorities.md` to capture what you learned about how the reviewer thinks. The strongest signal is **which comments the reviewer chose to post on the PR** — those represent findings they genuinely agreed with and considered worth the author's attention. Findings that were discussed but not posted were deprioritized for a reason.

Update this file after each review session by adding to or refining the existing content. Over time it should build a model of:
- What the reviewer consistently elevates (e.g. tenant scoping, proto compatibility, framework conformance)
- What the reviewer consistently deprioritizes (e.g. pure code design when it doesn't affect correctness)
- How the reviewer weighs trade-offs (e.g. pragmatic about over-engineering when scale is small)
- How the reviewer wants comments delivered (e.g. inline on specific lines, not summary comments)
- Patterns in the reviewer's questions and what they reveal about priorities

Format — free-form, written as evolving guidelines, updated in-place across sessions.

---

### Updating the index

After writing or updating any memory file, update `.claude/skills/config-service-code-review/memory/MEMORY.md`:
- Domain knowledge: one line per file — `- [Name](filename.md) — one-line hook`
- Reviewer priorities: a single line pointing to `reviewer_priorities.md` — update in place, don't add duplicate entries

---

## Memory Layout

```
.claude/skills/config-service-code-review/memory/
├── MEMORY.md                  # Index — loaded first
├── domain_*.md                # Domain knowledge (architecture, invariants)
└── reviewer_priorities.md     # Evolving model of what the reviewer prioritizes
```

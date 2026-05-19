---
name: pr-scope-discipline
type: domain-knowledge
---

Reviewers consistently push back on PRs that grow too broad. The skill should call this out when scope creep is visible.

## Rules

- **One concern per PR.** Reviewers separate api/proto changes from impl changes when both are non-trivial. *"Let's have the implementation in separate PR plz (only config-service api and Modsec variables changes in this PR)."*
- **Defer extra functionality if not required for the basic feature.** *"Be careful of taking on too much in this PR. Let's get the basics, but we don't want this growing to 2K lines (it's already too large to effectively review). Defer extra functionality if it's not required for basic UI, migration logic etc."*
- **Don't bundle unrelated cleanups** into a feature PR. If you noticed something nearby that needs fixing, file a follow-up.
- **Migration code, agent updates, and helm/values changes** should each typically be their own PR (or at minimum, separately reviewable commits).
- **Don't add features speculatively.** *"This is something we want to force users to enter — let's not add anything default now."* Reviewers prefer the smallest change that satisfies the immediate need.

## Cross-PR sequencing

- Proto-only PR first → impl PR second is a recurring pattern, especially for changes the agent will consume. This lets the agent codegen and start integrating before the platform impl ships.
- For breaking changes: introduce the new field/RPC, migrate consumers, *then* deprecate the old. Don't do all three in one PR.

## Reviewer phrasing

- "Let's not do this here — separate PR"
- "This is already too large to effectively review"
- "Defer this to a follow-up"
- "Take this in the next PR"
- "Out of scope"
- "Pre-existing, but..." (a flag that the reviewer noticed but isn't blocking the PR over it)

## How to apply during review

- **Comment on overall PR scope** when the diff exceeds ~500 lines AND mixes proto + impl + tests for multiple distinct concerns.
- **Flag** when a PR title says "X" but the diff also includes substantial unrelated changes (drive-by refactors, naming touchups, dependency bumps).
- **Don't block** small drive-by fixes — those are encouraged when in the same area.

**Why it matters for review:** Big PRs miss bugs because reviewers can't keep the full scope in their head. The reviewers explicitly call this out as a top-level concern.

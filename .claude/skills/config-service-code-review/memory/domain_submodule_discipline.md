---
name: submodule-discipline
type: domain-knowledge
---

`hypertrace-config-service` is included as a git submodule. Accidental submodule pointer changes show up in PRs regularly and are reliably caught by reviewers — the skill should catch them too.

## The pattern reviewers flag

A PR includes a change to the submodule pointer (the SHA recorded in the parent repo's tree for the submodule path) that:
- Is **unrelated** to the PR's stated purpose
- Points to a commit that's **not on the submodule's `main` branch** (CI enforces this — will fail)
- Was likely introduced by `git pull` without `--recurse-submodules` followed by `git add .`

## How reviewers handle it

- *"Is this submodule change intended? The commit in main branch is `<sha>`. So we should merge it to that if we didn't intend to make the change. I think you can checkout the commit in main in the `hypertrace-config-service`?"*
- Fix: `git pull --recurse-submodules` from main, or `git submodule update --init --recursive` followed by checking out the right SHA in the submodule.

## How to apply during review

If the PR diff shows changes to `hypertrace-config-service` (the submodule pointer):
- **Verify the new SHA is on the submodule's main branch** — if not, **P0** (CI will fail; user-facing comment).
- **Verify the change is intentional** — if the PR title/description doesn't mention a submodule update, ask the author whether it's intentional. Most of the time it isn't.
- **P2** if the submodule update is intentional but unrelated to the PR's main purpose — should be a separate PR per scope discipline rules.

## Detection

In the diff:
```
Subproject commit <old_sha>
Subproject commit <new_sha>
```
or filename `hypertrace-config-service` showing as modified with no actual file diff visible.

**Why it matters for review:** This is a recurring real bug — a PR aiming to add a feature also accidentally rolls back or rolls forward the submodule, breaking unrelated functionality. Easy to catch in review, easy to miss locally.

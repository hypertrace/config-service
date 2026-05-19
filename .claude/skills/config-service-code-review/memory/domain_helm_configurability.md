---
name: helm-configurability
type: domain-knowledge
---

When values are hardcoded, the reviewers' reflex is: "make it configurable via Helm." This is one of the most repeated patterns in this repo.

## What gets flagged for Helm-ification

- **Default rule sets / ignore lists / allow lists** — anything that may need to be tuned per-customer should be Helm-driven, not in code.
  - *"shouldn't we drive this via config? something like `ignoreDefaultDataTypes {rasp = ["db_query_attributes]}` in helm or some FF instead of baking into the code?"*
- **Cache TTLs / refresh intervals / timeouts** — magic numbers without rationale.
- **Feature toggles** — even pre-FF-system feature flags. Reviewers prefer a config-driven enable/disable over hardcoded behavior.
- **Limits / thresholds** that may differ between deployments (per-tenant rate limits, max items, etc.).

## When NOT to Helm-ify

- **Internal constants** that are intentionally fixed across all deployments (e.g. min TPA version constants for agent compatibility — these are compile-time facts, not deployment config).
- **Trivial single-use values** where adding a Helm value just adds noise.

## How values flow

- Helm values → Kubernetes ConfigMap → mounted as `application.conf` → parsed by Typesafe Config → injected via Guice.
- New configurable values typically need:
  - A field in `application.conf` (and the corresponding `*.conf` reference)
  - A Helm template change (in `helm/`)
  - A Guice `@Provides` or `@Named` injection point

## Helm chart changes

- **Helm chart and code change usually ship together** — if a code change requires a new env var or config key, the Helm chart needs the matching update. Reviewers verify both halves are present.
- **Reviewers** specifically push back when a code-level constant should be in `application.conf` instead.

## How to apply during review

- **P2** when a new hardcoded threshold/timeout/list is added that customers may want to tune.
- **P2** when a Helm-controlled value is added in code without a matching Helm chart change.
- **P3** when a comment says "TODO: make configurable" — reviewers prefer doing it now over later.
- **Don't flag** internal compatibility constants (TPA versions, proto field numbers, etc.) — those should stay in code.

**Why it matters for review:** Customers often need per-tenant tuning. Hardcoded values force a code change + redeploy for what should be a Helm values bump. The reviewers raise this reflexively.

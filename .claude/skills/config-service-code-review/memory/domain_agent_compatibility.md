---
name: agent-compatibility
type: domain-knowledge
---

The agent is a primary consumer of this service's protos and APIs. Agent-impacting changes get the most scrutiny.

## Patterns the reviewers consistently flag

- **Anything sent to the agent must be in agent-friendly format at the API boundary.** If the platform stores it differently, the translation happens on the read path, not in the agent. *"Anything going to the agent needs to be in the first format, meaning the translation of an enum needs to happen somewhere."*
- **Never throw on an agent request.** *"We never want to throw on an agent request. Otherwise, all agents would be broken if a new type were introduced without support. Let's make sure it fails gracefully."* — silent skip / empty list / sensible default is preferred over throwing.
- **Adding a new enum value risks breaking older agents** if they switch on the enum without a default. Reviewers ask: "do we need an agent-side change?" / "should this be ordinals or strings?" / "what's the min TPA version?"
- **Min TPA version gates**: when behavior changes for the agent, look for a `*_MIN_TPA_VERSION` constant or feature flag gating the new behavior so older agents still work.
- **Default rules**: rules the agent uses out-of-the-box (default rules) are NOT typically returned by platform-facing query APIs. If a PR changes default rule behavior, verify the agent's read path, not just the platform's.
- **Hash-based change detection**: agent flows often use a hash to denote "config unchanged, no need to ship a blob". Adding a field to a message that goes to the agent may invalidate caching unless the hash is recomputed correctly.
- **Don't add fields purely for agent communication into config-service** if they belong in agent-specific service (e.g. AST-specific service for CLI state). Reviewers push back on putting agent state in config-service.

## Reviewer questions to anticipate

- "Is this consumed by the agent? Is there a corresponding agent change?"
- "What happens for old agents that don't know about this new field/enum?"
- "Is this gated by a TPA version or feature flag?"
- "Will this break the hash-based change detection?"
- "Should this translation happen here or in the agent?"

## How to apply during review

- **P0** if a proto field/enum used by the agent is renamed, renumbered, or has type changed without explicit agent migration plan.
- **P0** if an RPC reachable by the agent throws on unknown enum/type instead of degrading gracefully.
- **P2** if a new agent-facing enum value/field lacks a min-TPA-version gate when behavior changes.
- **P3** if state/data that doesn't belong here (e.g. agent process state) is being added to config-service.

**Why it matters for review:** Breaking the agent ships broken telemetry/blocking to every customer simultaneously. The reviewers treat agent-facing changes as the highest-risk surface in this repo.

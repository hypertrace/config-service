---
name: aes-activity-log-xref
description: >
  Cross-reference: adding activity/action logs for a config-service config type.
  Triggers on "add activity log", "onboard action log", "action logging for config type".
  Redirects to the aes-onboard-activity-log skill in activity-event-service.
---

# Activity Log Onboarding (Cross-Reference)

This skill lives in `activity-event-service`. It automates adding activity/action
logs for any config-service config type across `activity-event-service` (backend)
and `graphql-service` (GQL layer).

## How to Use

```bash
cd $CODEBASE_ROOT/activity-event-service
```

Then say "add activity log for X" or paste a config-service proto link.

The skill (`aes-onboard-activity-log`) will:
1. Fetch and parse the config proto
2. Generate change proto, converter, tests, and registrations in activity-event-service
3. Generate GQL interface, extractor, and type registry in graphql-service
4. Look up iam-v2 permissions for access control
5. Open PRs in both repos

## Reference

- Skill location: `$CODEBASE_ROOT/activity-event-service/.claude/skills/aes-onboard-activity-log/SKILL.md`
- Backend example PR: [activity-event-service #1113](https://github.com/Traceableai/activity-event-service/pull/1113)
- GQL example PR: [graphql-service #4252](https://github.com/Traceableai/graphql-service/pull/4252)

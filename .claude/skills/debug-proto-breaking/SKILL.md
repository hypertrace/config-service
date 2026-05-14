---
name: debug-proto-breaking
description: Diagnose and explain proto breaking changes detected by buf, suggest fixes, and explain impact
globs:
  - "buf.yaml"
  - "AGENTS.md"
  - ".github/workflows/build.yml"
  - "**/*-config-service-api/src/main/proto/**/*.proto"
---

# Debug Proto Breaking Changes

Diagnose and explain proto breaking changes detected by buf, suggest safe alternatives, and explain the impact of proto modifications on deployed services.

## When to Use

- CI fails with "buf breaking" errors on your PR
- User modified a .proto file and needs to understand the impact
- Need to understand why a specific proto change is considered breaking
- Need guidance on safe proto evolution patterns
- Want to check if a proto change will break backwards compatibility

## When NOT to Use

- Adding a new proto file from scratch (no breaking change concerns)
- Adding new fields to existing messages (generally safe, but verify with buf)
- Use `find-config-service-usage` to understand service architecture first

## Required Information

Before starting, ask the user for:

- **Service name or proto file path**: Which proto file has the breaking change?
- **What change was made** (optional): If known, describe the modification (e.g., "renamed field", "removed enum value")
- **Buf error message** (optional): Paste the error message from CI or `buf breaking` command
- **Intended goal**: What were you trying to accomplish with this change?

## Steps

### 1. Reproduce the Breaking Change Error

Run buf breaking check:
```bash
buf breaking --against .git#branch=main
```

This compares current proto files against the main branch to detect breaking changes.

**Example error output:**
```
certificate_management_config.proto:45:3: Field "1" on message "Certificate" changed name from "certificate_id" to "id".
certificate_management_config.proto:52:3: Field "5" with name "status" on message "Certificate" changed type from "CertificateStatus" to "string".
```

### 2. Identify the Specific Breaking Change Type

Common breaking change categories:

**Field Changes:**
- ❌ **Field removed**: Deleting a field breaks deserialization
- ❌ **Field renamed**: Changes field name (wire format uses field numbers, but JSON API breaks)
- ❌ **Field number changed**: Breaks wire format compatibility
- ❌ **Field type changed**: `int32 → string` breaks deserialization
- ❌ **Required → Optional**: Can break validation logic
- ✅ **Field added**: Generally safe (backward compatible)
- ✅ **Optional → Required**: Safe for new fields, risky for existing

**Enum Changes:**
- ❌ **Enum value removed**: Breaks if existing data uses that value
- ❌ **Enum value number changed**: Breaks wire format
- ❌ **Enum renamed**: May break JSON API
- ✅ **Enum value added**: Generally safe

**Message Changes:**
- ❌ **Message removed**: Breaks if referenced anywhere
- ❌ **Message renamed**: Breaks imports
- ❌ **Nested message moved**: Changes fully-qualified name
- ✅ **Message added**: Safe

**Service Changes:**
- ❌ **RPC removed**: Breaks existing clients
- ❌ **RPC renamed**: Breaks existing clients
- ❌ **RPC request/response type changed**: Breaks compatibility
- ✅ **RPC added**: Safe (new functionality)

### 3. Explain Why It's Breaking

For each breaking change, explain the impact:

**Example explanations:**

**Field renamed:**
```
BREAKING: Field "certificate_id" renamed to "id"

Why it breaks:
- Existing clients sending {"certificate_id": "abc"} will fail
- JSON/gRPC-Web clients rely on field names, not just field numbers
- Stored configuration data with old field name can't be deserialized

Impact:
- All services calling CertificateManagementConfigService must be updated
- Existing certificate configurations in database may become inaccessible
- API documentation and client SDKs must be regenerated
```

**Field type changed:**
```
BREAKING: Field "status" type changed from "CertificateStatus" enum to "string"

Why it breaks:
- Wire format is incompatible (enum is int32, string is bytes)
- Existing data with enum values (0, 1, 2) can't deserialize to string
- Clients expecting enum will fail validation

Impact:
- Database migration required to convert enum values to strings
- All callers must update their code to send strings
- Rollback becomes difficult after deployment
```

### 4. Suggest Safe Alternatives

Provide backwards-compatible alternatives:

**Instead of renaming a field:**
```protobuf
// ❌ DON'T: Rename field
message Certificate {
  string id = 1;  // Was: certificate_id
}

// ✅ DO: Add new field, deprecate old
message Certificate {
  string certificate_id = 1 [deprecated = true];
  string id = 6;  // New field number
}
```

**Instead of changing field type:**
```protobuf
// ❌ DON'T: Change field type
message Certificate {
  string status = 5;  // Was: CertificateStatus enum
}

// ✅ DO: Add new field with new type
message Certificate {
  CertificateStatus status = 5 [deprecated = true];
  string status_string = 6;  // New field
}
```

**Instead of removing a field:**
```protobuf
// ❌ DON'T: Remove field
message Certificate {
  // Removed: string description = 3;
}

// ✅ DO: Deprecate field, keep field number reserved
message Certificate {
  reserved 3;
  reserved "description";
  // Field can be safely removed in future major version
}
```

**Instead of removing enum value:**
```protobuf
// ❌ DON'T: Remove enum value
enum CertificateStatus {
  CERTIFICATE_STATUS_UNSPECIFIED = 0;
  CERTIFICATE_STATUS_ACTIVE = 1;
  // Removed: CERTIFICATE_STATUS_PENDING = 2;
}

// ✅ DO: Deprecate enum value
enum CertificateStatus {
  CERTIFICATE_STATUS_UNSPECIFIED = 0;
  CERTIFICATE_STATUS_ACTIVE = 1;
  CERTIFICATE_STATUS_PENDING = 2 [deprecated = true];
}
```

### 5. Check Impact Scope

Determine who is affected:

1. **Find service usage:**
   - Use `find-config-service-usage` skill to identify consumers
   - Check which services call this gRPC service
   - Check if proto types are used in other services

2. **Check deployment status:**
   - Are there deployed services using the current proto?
   - Is this a new service (not yet deployed)?
   - What's the rollout/rollback strategy?

3. **Assess migration path:**
   - Can old and new versions coexist?
   - Is database migration required?
   - How many clients need updates?

### 6. Document the Decision

If breaking change is justified, document why:

**In PR description:**
```markdown
## Proto Breaking Change

**Change**: Renamed field `certificate_id` to `id`

**Justification**: 
- Service is not yet deployed to production
- No external clients exist
- Change improves API consistency with other services

**Impact**:
- ✅ No production impact (service unreleased)
- ⚠️  Test data in staging will be incompatible
- ✅ All internal code updated in this PR

**Buf Override**: Breaking change approved by team lead
```

## Key File Locations

| File | Purpose | Location |
|------|---------|----------|
| buf.yaml | Buf configuration and breaking rules | `/buf.yaml` (root) |
| Proto files | Service and message definitions | `*-config-service-api/src/main/proto/**/*.proto` |
| CI Workflow | Proto validation in CI | `.github/workflows/build.yml` |
| AGENTS.md | Proto guidelines | `/AGENTS.md` (section: Protocol Buffers) |

## Common Pitfalls

- **Assuming field numbers don't matter**: They're critical for wire format compatibility
- **Thinking enum renames are safe**: They break JSON API clients
- **Not checking deployed services**: Breaking changes affect production
- **Forgetting about stored data**: Database has serialized protos that must deserialize
- **Adding required fields to existing messages**: Breaks deserialization of old data without that field

## Presenting Findings

Format your findings clearly:

```markdown
# Proto Breaking Change Analysis

## Summary
- **File**: [proto file path]
- **Change Type**: [Field renamed / Type changed / etc.]
- **Severity**: [High / Medium / Low]

## Breaking Changes Detected

1. **[Change description]**
   - **Location**: [file:line]
   - **Why it breaks**: [explanation]
   - **Impact**: [who/what is affected]

## Recommended Fix

### Option 1: Backwards-Compatible Evolution (Recommended)
[Code example with deprecation]

**Pros**: [benefits]
**Cons**: [tradeoffs]

### Option 2: Accept Breaking Change (If Justified)
[Justification requirements]

**Required**:
- [ ] Team lead approval
- [ ] No production services using current proto
- [ ] Migration plan documented
- [ ] PR description explains breaking change

## Next Steps
1. [Action item 1]
2. [Action item 2]
```

## Agent-Specific Context

**Buf Configuration:**
- Location: `buf.yaml` at repository root
- Validates breaking changes against main branch
- Runs automatically in CI for all PRs (`.github/workflows/build.yml`)

**Proto Evolution Best Practices:**
- Always use `[deprecated = true]` instead of removing
- Reserve field numbers and names after deprecation
- Use new field numbers for changed types
- Document breaking changes in PR descriptions
- Consider multi-phase rollout for major changes

**When Breaking Changes Are Acceptable:**
- Service not yet deployed to production
- Pre-release / alpha APIs
- Internal-only services with coordinated updates
- Major version bumps (v1 → v2)
- Urgent security fixes requiring incompatible changes

**Related Documentation:**
- AGENTS.md: "Protocol Buffers" section (lines discussing buf validation)
- buf.build documentation: https://buf.build/docs/breaking/overview
- Proto3 language guide: https://protobuf.dev/programming-guides/proto3/

## Example Scenarios

### Scenario 1: Field Renamed
**Error**: `Field "1" on message "Certificate" changed name from "certificate_id" to "id"`
**Fix**: Add new field `id = 6`, deprecate `certificate_id = 1`

### Scenario 2: Enum Value Removed
**Error**: `Enum value "2" on enum "CertificateStatus" was deleted`
**Fix**: Mark as `[deprecated = true]`, document migration path

### Scenario 3: Required Field Added
**Error**: `Field "7" with name "new_field" on message "Certificate" added without "optional"`
**Fix**: Make field `optional` to maintain backwards compatibility

### Scenario 4: Message Type Changed in RPC
**Error**: `RPC "GetCertificate" request type changed from "GetCertificateRequest" to "GetCertificateRequestV2"`
**Fix**: Create new RPC `GetCertificateV2` instead, deprecate old RPC

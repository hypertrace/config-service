---
name: find-config-service-usage
description: Trace where a config service is defined, implemented, registered, and consumed across the codebase
globs:
  - "**/*-config-service-api/src/main/proto/**/*.proto"
  - "**/*-config-service-impl/src/main/java/**/*ServiceImpl.java"
  - "traceable-config-service-factory/**/*.java"
  - ".github/CODEOWNERS"
---

# Find Config Service Usage

Trace where a specific config service is defined, implemented, registered, and consumed across the codebase. This skill helps you understand the complete architecture of a config service before making changes.

## When to Use

- Need to understand where a service is defined and used before modifying it
- Investigating dependencies and impact of proto changes
- Onboarding to understand a specific service's architecture
- Finding who owns a particular config service
- Understanding how a service is wired into the main service

## When NOT to Use

- Adding a new config service from scratch (use `add-config-service` instead)
- Debugging proto breaking changes (use `debug-proto-breaking` instead)

## Required Information

Before starting, ask the user for:

- **Service name**: The service to trace (e.g., "CertificateManagementConfigService", "ThreatScoringConfigService")
- **Focus area** (optional): Specific aspect to investigate
  - "definition" - Proto and gRPC service definition
  - "implementation" - Java implementation classes
  - "consumers" - Who calls this service
  - "all" (default) - Complete trace

## Steps

### 1. Find Proto Definition (API Module)

**Search pattern:** `**/*-config-service-api/src/main/proto/**/*.proto`

Look for:
- Service name in proto file (e.g., `service CertificateManagementConfigService`)
- RPC method definitions
- Request/Response message types
- Data model definitions

**Example output format:**
```
Proto Definition:
- Location: certificate-management-config-service-api/src/main/proto/ai/traceable/certificate/management/config/service/v1/certificate_management_config_service.proto
- Service: CertificateManagementConfigService
- RPCs: CreateCertificate, GetCertificates, UpdateCertificate, DeleteCertificate
- Main Types: Certificate, CertificateMetadata, CertificateStatusDetails
```

### 2. Find Service Implementation (Impl Module)

**Search pattern:** `**/*-config-service-impl/src/main/java/**/*ServiceImpl.java`

Look for:
- ServiceImpl class extending generated ImplBase
- Factory class with `build()` method
- Guice Module class extending AbstractModule
- Manager classes implementing business logic

**Example output format:**
```
Implementation:
- ServiceImpl: certificate-management-config-service-impl/.../CertificateManagementConfigServiceImpl.java
- Factory: certificate-management-config-service-impl/.../CertificateManagementConfigServiceFactory.java
- Module: certificate-management-config-service-impl/.../CertificateManagementConfigServiceModule.java
- Manager: certificate-management-config-service-impl/.../manager/CertificateConfigManagerImpl.java
- Store: certificate-management-config-service-impl/.../store/CertificateConfigStore.java
```

### 3. Find Factory Registration

**Search in:** `traceable-config-service-factory/src/main/java/ai/traceable/config/service/TraceableInternalConfigServiceFactory.java`

Look for:
- Import statement for Factory class (lines 3-66)
- Factory.build() call in buildServices() method (lines 91-395)
- Parameters passed to factory (channel, config, changeEventGenerator, etc.)

**Example output format:**
```
Factory Registration:
- Import: Line 20: import ai.traceable.certificate.management.config.service.v1.CertificateManagementConfigServiceFactory;
- Wiring: Lines 365-366:
  wrap(CertificateManagementConfigServiceFactory.build(
    providers.getLocalChannel(), providers.getChangeEventGenerator()))
```

### 4. Find Build Dependencies

**Search in:** `**/build.gradle.kts`

Look for:
- Module includes in settings.gradle.kts
- API dependencies (implementation, api, testImplementation)
- Which modules depend on this service

**Example output format:**
```
Build Configuration:
- Gradle Includes: settings.gradle.kts lines 59-60
  - include(":certificate-management-config-service-api")
  - include(":certificate-management-config-service-impl")
- Dependencies: traceable-config-service-factory/build.gradle.kts
  - implementation(projects.certificateManagementConfigServiceImpl)
```

### 5. Find Ownership

**Search in:** `.github/CODEOWNERS`

Look for team or individual ownership entries.

**Example output format:**
```
Ownership:
- Team: @certificate-team
- Pattern: /certificate-management-config-service-*
```

### 6. Find Cross-Service Dependencies (Optional)

Search for:
- Other services that call this service via gRPC
- Services that reference this service's proto types
- Validators or integrations with other services

**Example:**
```
Cross-Service Dependencies:
- Used by: cloud-edge-deployment-config-service (checks certificate usage before deletion)
- Integration: CertificateUsageValidator.java validates against CloudEdgeDeploymentConfigService
```

## Key File Locations

| Layer | Search Pattern | Example |
|-------|----------------|---------|
| Proto Service | `**/*-config-service-api/src/main/proto/**/*_config_service.proto` | Find `service [ServiceName]` declaration |
| Proto Models | `**/*-config-service-api/src/main/proto/**/*_config.proto` | Find message definitions |
| ServiceImpl | `**/*-config-service-impl/src/main/java/**/*ServiceImpl.java` | Find class extending `*ServiceImplBase` |
| Factory | `**/*-config-service-impl/src/main/java/**/*ServiceFactory.java` | Find `build()` method |
| Module | `**/*-config-service-impl/src/main/java/**/*ServiceModule.java` | Find Guice bindings |
| Settings | `settings.gradle.kts` | Find `include(":*-config-service-api")` |
| Factory Wiring | `traceable-config-service-factory/.../TraceableInternalConfigServiceFactory.java` | Find import and `Factory.build()` call |
| CODEOWNERS | `.github/CODEOWNERS` | Find ownership pattern |

## Common Pitfalls

- **Multiple services with similar names**: Be specific with search terms (e.g., "CertificateManagement" vs "Certificate")
- **Versioned services**: Some services have v1 and v2 implementations (check both)
- **Nested packages**: Service implementations may be in versioned packages (v1/, v2/)
- **Factory variants**: Some factories return `BindableService`, others return `Set<BindableService>`

## Presenting Findings

Format your findings in a structured way:

```markdown
# [Service Name] Architecture

## Proto Definition
- **Location**: [path]
- **Package**: [proto package]
- **Service**: [service name]
- **RPCs**: [list of methods]
- **Key Types**: [main message types]

## Implementation
- **Module**: [module name]
- **ServiceImpl**: [path to ServiceImpl.java]
- **Factory**: [path to Factory.java]
- **Guice Module**: [path to Module.java]
- **Business Logic**: [Manager, Store, Validator classes]

## Registration
- **Settings**: [line numbers in settings.gradle.kts]
- **Factory Wiring**: [line numbers in TraceableInternalConfigServiceFactory.java]
- **Dependencies**: [which modules depend on this]

## Ownership
- **Team**: [team name or individuals]
- **CODEOWNERS Pattern**: [ownership pattern]

## Cross-Service Dependencies
- **Uses**: [services this service calls]
- **Used By**: [services that call this service]
- **Integrations**: [special integration points]
```

## Agent-Specific Context

**Repository Scale:**
- 100+ config services registered
- 427-line factory file with 50+ service imports
- 153+ module includes in settings.gradle.kts

**Search Tips:**
- Use service name without "Service" suffix for broader search
- Check both PascalCase (Java) and snake_case (proto) naming
- Look for Factory import and build() call together
- CODEOWNERS may use directory wildcards

**Example Services by Complexity:**
- **Simple**: certificate-management (4 RPCs, single module)
- **Medium**: threat-management (multiple managers, validators)
- **Complex**: fraud-policy (cross-service calls, GrpcChannelRegistry)
- **Versioned**: blocking-config-service (v1 and v2 implementations)

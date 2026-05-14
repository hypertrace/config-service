---
name: add-config-service
description: Scaffold a new gRPC config service module following the established Factory → Module → ServiceImpl → Manager → Store pattern
globs:
  - "AGENTS.md"
  - "settings.gradle.kts"
  - "buf.yaml"
  - "traceable-config-service-factory/**/*.java"
  - "certificate-management-config-service-api/**/*.proto"
  - "certificate-management-config-service-impl/**/*.java"
---

# Add Config Service

Scaffold a new gRPC configuration service module following the established pattern used across 100+ existing config services in this repository.

## When to Use

- User asks to "add a new config service"
- User wants to "create a config service for [domain]"
- Need to scaffold a new service module from scratch
- Adding a new configuration domain to the platform

## When NOT to Use

- Modifying an existing config service (just edit the files directly)
- Adding fields to existing proto (use standard proto editing)
- Use `find-config-service-usage` if you need to understand an existing service first

## Required Information

Before starting, ask the user for:

- **Service domain name**: The domain this service manages (e.g., "threat-scoring", "certificate-management", "fraud-policy")
- **Brief description**: What configuration data does this service manage?
- **Proto package path**: Following pattern `ai.traceable.[domain].config.service.v1` (suggest based on domain name)
- **CRUD operations needed**: Which operations to implement (Create, Get, Update, Delete, List)?
- **Reference service** (optional): Similar existing service to use as template (default: certificate-management-config-service)

## Steps

Follow the 8-step process documented in AGENTS.md "When adding a new config service module":

### 1. Create API Module with Proto Definitions

Create directory: `[domain-name]-config-service-api/`

**Proto files to create in `src/main/proto/ai/traceable/[domain]/config/service/v1/`:**
- `[name]_config_service.proto` - gRPC service definition with RPC methods
- `[name]_config.proto` - Data model definitions (main entity, filters, etc.)
- `[name]_config_request_response.proto` - Request/response messages

**build.gradle.kts:**
```gradle
plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.traceable.publish)
}

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:3.24.0" }
  plugins { id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:1.58.0" } }
  generateProtoTasks {
    all().forEach { task ->
      task.plugins { id("grpc") }
    }
  }
}

dependencies {
  api(commonLibs.bundles.grpc.api)
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc")
    }
  }
}
```

### 2. Create Implementation Module

Create directory: `[domain-name]-config-service-impl/`

**Java files to create in `src/main/java/ai/traceable/[domain]/config/service/`:**
- `[Module]ConfigServiceFactory.java` - Factory with `build()` method returning BindableService
- `[Module]ConfigServiceModule.java` - Guice module extending AbstractModule
- `[Module]ConfigServiceImpl.java` - Service implementation extending generated ImplBase

**build.gradle.kts:**
```gradle
plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.[domainName]ConfigServiceApi)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeevent)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  
  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test { useJUnitPlatform() }
```

### 3. Add Modules to settings.gradle.kts

Add two lines after existing includes (around line 46-213):
```gradle
include(":[domain-name]-config-service-api")
include(":[domain-name]-config-service-impl")
```

### 4. Register Proto in buf.yaml

Add proto directory to buf.yaml lint configuration if using custom proto paths.

### 5. Wire Service into TraceableInternalConfigServiceFactory.java

**Add import** (top of file, around lines 3-66):
```java
import ai.traceable.[domain].config.service.[Module]ConfigServiceFactory;
```

**Add factory call** in `buildServices()` method (around lines 91-395):
```java
wrap(
    [Module]ConfigServiceFactory.build(
        providers.getLocalChannel(),
        providers.getChangeEventGenerator())),
```

### 6. Add Dependency in traceable-config-service-factory/build.gradle.kts

Add implementation dependency:
```gradle
implementation(projects.[domainName]ConfigServiceImpl)
```

### 7. Add Unit Tests

Create test files in `[domain-name]-config-service-impl/src/test/java/`:
- `[Module]ConfigServiceImplTest.java`
- `[Module]ConfigServiceModuleTest.java`
- Manager and validator tests as needed

### 8. Update CODEOWNERS (if applicable)

Add team ownership entry in `.github/CODEOWNERS`:
```
/[domain-name]-config-service-* @team-name
```

## Key File Locations

| Layer | File Pattern | Example |
|-------|--------------|---------|
| Proto Service | `*-api/src/main/proto/ai/traceable/[domain]/config/service/v1/[name]_config_service.proto` | `certificate-management-config-service-api/src/main/proto/.../certificate_management_config_service.proto` |
| Proto Data | `*-api/src/main/proto/ai/traceable/[domain]/config/service/v1/[name]_config.proto` | `certificate-management-config-service-api/src/main/proto/.../certificate_management_config.proto` |
| Factory | `*-impl/src/main/java/ai/traceable/[domain]/config/service/[Module]ConfigServiceFactory.java` | `certificate-management-config-service-impl/.../CertificateManagementConfigServiceFactory.java` |
| Guice Module | `*-impl/src/main/java/ai/traceable/[domain]/config/service/[Module]ConfigServiceModule.java` | `certificate-management-config-service-impl/.../CertificateManagementConfigServiceModule.java` |
| Service Impl | `*-impl/src/main/java/ai/traceable/[domain]/config/service/[Module]ConfigServiceImpl.java` | `certificate-management-config-service-impl/.../CertificateManagementConfigServiceImpl.java` |
| Settings | `settings.gradle.kts` (root) | Lines 46-213 show all module includes |
| Factory Wiring | `traceable-config-service-factory/src/main/java/ai/traceable/config/service/TraceableInternalConfigServiceFactory.java` | Line 365-366 shows certificate service wiring |

## Common Pitfalls

- **Forgetting settings.gradle.kts**: Service won't be included in build
- **Not updating TraceableInternalConfigServiceFactory**: Service won't be available at runtime
- **Proto package mismatch**: Ensure proto package matches Java package structure
- **Missing buf.yaml registration**: Proto linting may fail
- **Incorrect Guice bindings**: Service injection will fail at runtime
- **Not running `./gradlew spotlessApply`**: CI will fail on code formatting

## Agent-Specific Context

**Universal Pattern Across 100+ Services:**
- Factory → Module → ServiceImpl → Manager → Store
- All services use Guice for dependency injection
- All services extend generated gRPC ImplBase
- All services registered in settings.gradle.kts and TraceableInternalConfigServiceFactory.java

**Reference Implementations:**
- **Simple service**: `certificate-management-config-service` (2539 lines, 4 CRUD operations)
- **Complex service**: `fraud-policy-config-service` (uses GrpcChannelRegistry for cross-service calls)
- **Versioned service**: `blocking-config-service` (has v1 and v2 implementations)

**Build Commands to Test:**
```bash
# Build specific module
./gradlew :[domain-name]-config-service-api:build
./gradlew :[domain-name]-config-service-impl:build

# Run tests
./gradlew :[domain-name]-config-service-impl:test

# Full build
./gradlew build

# Format code
./gradlew spotlessApply
```

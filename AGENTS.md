# AGENTS.md - Config Service

Instructions for AI coding agents working in the config-service repository.

## Project Overview

Config Service is a large multi-module Gradle project containing 100+ configuration services built on top of the generic config-service framework from Hypertrace. It provides gRPC-based configuration management for Traceable's API security and observability platform, including sensitive data configuration, API gateway configuration, anomaly detection, bot protection, fraud detection, threat management, integrations, and many other platform services.

**Architecture:**
- **100+ service modules** - Each domain has `-api` (proto definitions) and `-impl` (implementation) modules
- **hypertrace-config-service** - Base generic config service framework (included as git submodule)
- **traceable-config-service** - Main service deployment aggregating all config services
- **Client modules** - Specialized clients for caching and config access (e.g., `feature-caching-client`, `data-classification-client`)
- **Utility modules** - Shared utilities (`config-utils`, `config-proto-utils`, `audit-utils`, `modsecurity-utils`)

**Technology Stack:**
- Java (language level defined by Hypertrace convention plugins)
- Gradle with Kotlin DSL
- gRPC for service interfaces
- Protocol Buffers for data models
- Lombok for boilerplate reduction
- Buf for protobuf linting and breaking change detection
- JUnit 5 for testing
- Mockito for mocking
- Guice for dependency injection

**Module naming pattern:**
- `*-config-service-api` - Proto definitions and gRPC service interfaces
- `*-config-service-impl` - Service implementations
- `*-config-service-*-client` - Specialized clients (caching, etc.)
- `*-utils` - Utility modules

## Build System

This is a massive multi-module Gradle project using Kotlin DSL for build configuration.

**Build commands:**
```bash
# Clean build with all checks
./gradlew clean build

# Build all modules
./gradlew build

# Assemble without tests (faster)
./gradlew assemble

# Build specific module
./gradlew :module-name:build

# Build Docker images
./gradlew dockerBuildImages

# Run tests with coverage
./gradlew check jacocoTestReport

# Run integration tests with coverage
./gradlew jacocoIntegrationTestReport

# Copy test reports on failure
./gradlew copyAllReports --output-dir=/tmp/test-reports
```

**Key Gradle features:**
- Uses Hypertrace convention plugins for Java codestyle
- Uses Traceable BOM for dependency management via custom version catalog (version: 0.4.5674)
- OWASP dependency check enabled (fails on CVSS >= 7.0)
- Includes `hypertrace-config-service` as composite build
- Custom suppressions in `owasp-suppressions.xml`
- Parallel builds enabled for performance
- Gradle daemon and caching enabled

## Testing

**Test structure:**
- Unit tests in `src/test/java`
- Integration tests in `src/integrationTest/java`
- Test fixtures in `src/testFixtures/java`
- Test resources in `src/test/resources` and `src/integrationTest/resources`

**Test commands:**
```bash
# Run all unit tests
./gradlew test

# Run tests in specific module
./gradlew :module-name:test

# Run specific test class
./gradlew test --tests "ClassName"

# Run specific test method
./gradlew test --tests "ClassName.testMethodName"

# Run with coverage
./gradlew check jacocoTestReport

# Run integration tests
./gradlew integrationTest

# Run integration tests with coverage
./gradlew jacocoIntegrationTestReport
```

## Code Style

**Conventions:**
- Lombok for boilerplate reduction (`@Slf4j`, `@Inject`, `@Singleton`)
- Constructor injection with `@Inject` (not field injection)
- Spotless for code formatting via Hypertrace codestyle plugin
- Follow Hypertrace Java codestyle conventions

**Linting & Formatting:**
```bash
# Check code style
./gradlew spotlessCheck

# Auto-fix code style
./gradlew spotlessApply

# Run before committing
./gradlew spotlessApply build
```

## Protocol Buffers

**Proto files location:** `*-api/src/main/proto/`

**Proto tooling:**
- Buf for linting and breaking change detection
- Configuration: `buf.yaml` at root, `buf.gen.yaml` for code generation

**Proto commands:**
```bash
# Lint protos
buf lint

# Check for breaking changes (against main)
buf breaking --against .git#branch=main

# Build protos (happens automatically with Gradle build)
./gradlew build
```

**IMPORTANT:** Breaking proto changes require careful justification. The `buf breaking` check runs automatically in CI on every PR.

## CI/CD

**GitHub Actions workflows:**
- `build.yml` - Main build, test, proto validation, helm chart validation, dependency check, security scans
- `publish.yml` - Publish artifacts on merge to main
- `static-analysis.yml` - Code quality checks (SonarQube, etc.)

**PR build stages:**
1. Validate protos (buf lint, buf breaking) - PR only
2. Validate Helm charts
3. Build and Docker image creation
4. Unit tests with coverage
5. Integration tests with coverage
6. Dependency check (OWASP)
7. Docker image security scanning (Trivy)

**Deployment:**
- Deployed as `traceable-config-service` Docker image
- Helm charts in `helm/` directory
- GCP Artifact Registry for Docker images and Maven artifacts

## Git Workflow

**Submodules:**
- This repo includes `hypertrace-config-service` as a git submodule
- Always fetch with `--recursive` flag: `git clone --recursive` or `git submodule update --init --recursive`
- CI validates submodules point to commits from main branch

**Branch naming:** `JIRA-ID-description` or `NO-TICKET-description` (e.g., `AAP-1234-add-fraud-config`, `NO-TICKET-fix-typo`)

**Commit format:** `JIRA-ID: Description` or `NO-TICKET: Description`

**PR title format:** Same as commit format

**Default branch:** `main`

**PR requirements:**
- All CI checks must pass (build, test, proto validation, dependency check, security scan)
- Breaking proto changes require justification and discussion
- Submodules must point to commits from main branch (validated in CI)

## Development Guidelines

**When adding a new config service module:**
1. Create `-api` module with proto definitions in `src/main/proto/`
2. Add to `settings.gradle.kts` includes
3. Create `-impl` module with gRPC service implementation
4. Implement service extending generated gRPC base class
5. Create Guice module extending `AbstractModule` for dependency injection bindings
6. Add unit tests in `src/test/java`
7. Add integration test in `src/integrationTest/java` if needed
8. Wire service into `traceable-config-service` main service if needed

**When modifying protos:**
1. Check for breaking changes: `buf breaking --against .git#branch=main`
2. If breaking, document justification and discuss with team
3. Update both proto and implementation code
4. Run full build to ensure generated code compiles: `./gradlew build`
5. Update tests to reflect proto changes

**When adding dependencies:**
1. Check if already available in Traceable BOM version catalog
2. If adding new dependency, discuss due to OWASP dependency check constraints
3. CVSS scores >= 7.0 will fail the build
4. Use `owasp-suppressions.xml` only for false positives with clear justification

## Claude Code Rules

Coding rules (style, git, testing) are in `.claude/rules/`:
- `code-style.md` - Java code style conventions
- `git.md` - Git workflow and branch management
- `testing.md` - Testing guidelines and practices

## Claude Code Skills

This repo includes agent skills for common development tasks. Skills can be invoked with `/skill-name` or via the skills menu. See `.claude/skills/` for full step-by-step guides:

- **`add-config-service`** — Scaffold a new gRPC config service following the Factory → Module → ServiceImpl → Manager → Store pattern used across 100+ services
- **`find-config-service-usage`** — Trace where a config service is defined, implemented, registered, and consumed across the codebase
- **`debug-proto-breaking`** — Diagnose and explain proto breaking changes detected by buf, suggest fixes, and explain impact

## Common Tasks

**Add a new gRPC config service:**
1. Create `my-feature-config-service-api` module with proto in `src/main/proto/`
2. Create `my-feature-config-service-impl` module with implementation
3. Add both to `settings.gradle.kts`
4. Implement gRPC service extending generated base class
5. Create Guice module for dependency injection
6. Add unit and integration tests
7. Wire into main service deployment if needed

**Add a new proto message or RPC:**
1. Update proto file in `-api` module
2. Run `buf breaking` to check for breaking changes
3. Run `./gradlew build` to regenerate code
4. Update implementation to use new proto definitions
5. Add tests

**Debug build issues:**
1. Check Gradle build output for errors
2. Verify proto generation succeeded
3. Check dependency conflicts: `./gradlew dependencies`
4. Check OWASP dependency check report if build fails on CVSS
5. For specific module: `./gradlew :module-name:build --info`

**Debug test failures:**
1. Run specific test: `./gradlew :module-name:test --tests "ClassName.testMethod"`
2. Check test reports: `build/reports/tests/test/index.html`
3. Copy all test reports: `./gradlew copyAllReports --output-dir=/tmp/test-reports`
4. Check for proper mocking and test setup

## DOs

- Follow existing code patterns in the codebase
- Run tests before committing (`./gradlew test`)
- Run code formatting before committing (`./gradlew spotlessApply`)
- Use descriptive commit messages with ticket references
- Check which module(s) are affected by your changes
- Run tests for all affected modules
- Use constructor injection with `@Inject` for Guice dependency injection
- Run `buf breaking` before modifying proto files
- Check OWASP dependency check before adding new dependencies

## DON'Ts

- Never force push to `main`
- Never commit secrets, `.env` files, or credentials
- Never skip git hooks (`--no-verify`)
- Never run destructive commands without confirmation
- Don't add dependencies without checking for conflicts and OWASP CVSS scores
- Don't use field injection — always use constructor injection
- Don't modify proto files without running `buf breaking`
- Don't commit build output or generated code

## Commands to Never Run

- `git push --force origin main`
- `git commit --no-verify` or `git push --no-verify`
- `rm -rf /` (or any destructive recursive delete)

## Important Notes

- **Large codebase**: 100+ modules means builds can be slow. Use targeted module builds when possible: `./gradlew :module-name:build`
- **Git submodules**: Always work with `--recursive` flag. CI enforces submodules point to main branch commits
- **Proto backwards compatibility**: Breaking changes impact deployed services. Buf validates this automatically
- **OWASP dependency check**: CVSS >= 7.0 fails build. Use suppressions sparingly with justification
- **Lombok config**: `lombok.config` at root sets project-wide Lombok behavior including `@Named` annotation copying
- **Docker builds**: Docker context is in `traceable-config-service/build/docker` after `dockerBuildImages`
- **Integration tests**: Some integration tests require external dependencies (MongoDB, etc.)
- **Gradle performance**: Parallel builds, daemon, and caching enabled in `gradle.properties`

## Additional Resources

- Generic config-service framework: https://github.com/hypertrace/config-service
- Traceable BOM version catalog: ai.traceable.bom:traceable-version-catalog:0.4.5674
- Buf documentation: https://buf.build/docs

# AGENTS.MD - Config Service

## Project Overview

Config Service is a large multi-module Gradle project containing numerous configuration services built on top of the generic config-service framework. It provides configuration management for various features including sensitive data configuration, API gateway configuration, anomaly detection, bot protection, fraud detection, and many other platform services.

This is a massive multi-module Gradle project using Kotlin DSL for build configuration.

## Build System

- **Build tool**: Gradle (Kotlin DSL)
- **Build all**: `./gradlew build`
- **Clean build**: `./gradlew clean build`

## Testing

- **Run all tests**: `./gradlew test`
- **Run tests in specific module**: `./gradlew :module-name:test`
- **Run specific test class**: `./gradlew test --tests com.example.ClassName`

## Linting & Formatting

- **Check code style**: `./gradlew spotlessCheck`
- **Auto-fix code style**: `./gradlew spotlessApply`
- **Run before committing**: Always run `./gradlew spotlessApply build`

## Git Workflow

- **Branch naming**: `JIRA-TICKET-short-description` or `NO-TICKET-short-description`
- **Commit format**: `JIRA-TICKET: Description` or `NO-TICKET: Description`
- **PR title format**: Same as commit format
- **Default branch**: `main`

## DOs

- Follow existing code patterns in the codebase
- Run tests before committing (`./gradlew test`)
- Run code formatting before committing (`./gradlew spotlessApply`)
- Use descriptive commit messages with ticket references
- Check which module(s) are affected by your changes
- Run tests for all affected modules

## DON'Ts

- Never force push to `main`
- Never commit secrets, `.env` files, or credentials
- Never skip git hooks (`--no-verify`)
- Never run destructive commands without confirmation
- Don't add dependencies without checking for conflicts across all modules

## Commands to Never Run

- `git push --force origin main`
- `git commit --no-verify` or `git push --no-verify`
- `rm -rf /` (or any destructive recursive delete)

## Additional Resources

- See parent repository (activity-event-service) for detailed Java coding standards
- Generic config-service framework: https://github.com/hypertrace/config-service

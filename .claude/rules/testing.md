# Testing

- **Write unit tests** for every new service implementation, provider, and business logic component
- **Write integration tests** for services that require external dependencies (MongoDB, gRPC, etc.) in `src/integrationTest/java`
- **Use JUnit 5** for all tests (`@Test`, `@BeforeEach`, `@AfterEach`, etc.)
- **Use Mockito** for mocking dependencies — mock external services and dependencies, not business logic
- **Test fixtures** can be placed in `src/testFixtures/java` for reuse across modules
- **Use real objects** where possible — only mock external gRPC clients, databases, and I/O operations
- **Test gRPC services**: Test both success paths and error handling (invalid arguments, exceptions, etc.)
- **Verify exact response fields** rather than just that no exception was thrown
- **Run tests with coverage** before committing: `./gradlew check jacocoTestReport`
- **Integration tests**: Use `./gradlew jacocoIntegrationTestReport` for integration test coverage

## Test Structure

- **Unit tests**: `src/test/java` — fast, isolated tests with mocked dependencies
- **Integration tests**: `src/integrationTest/java` — tests with real external dependencies
- **Test fixtures**: `src/testFixtures/java` — shared test utilities and fixtures
- **Test resources**: `src/test/resources` and `src/integrationTest/resources` — test data files

## Don'ts

- **Don't remove or weaken failing tests** — fix them instead
- **Don't use lenient mocking** — fix unnecessary stubbing at the source
- **Don't replicate business logic in tests** — use hardcoded expected values, not recomputed results
- **Don't skip tests** without good reason and team discussion
- **Don't test Lombok-generated code** — focus on business logic, not getters/setters
- **Don't commit commented-out tests** — either fix them or remove them

## Running Tests

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

# Copy test reports on failure
./gradlew copyAllReports --output-dir=/tmp/test-reports
```

---
name: test-patterns
type: domain-knowledge
---

## Unit vs integration boundary

- **Unit test = mock dependencies.** *"Pre-existing, but if this is a unit test - mock your dependencies."* If the test is constructing real instances of multiple classes, it's a "bigger" test, not a unit test.
- **Integration test = real MongoDB / real gRPC server.** Lives in `src/integrationTest/java`. Should NOT publish (kafka, downstream services). *"Integration test should not publish."*
- **No "greater-than-unit-less-than-integration"** middle layer per reviewer judgment — pick one. If you need to test multiple classes together, use Guice rather than constructing each by hand so changes don't ripple through every test.

## What to assert

- **Compare full objects, don't unwrap piece by piece.** *"Testing it piecemeal just introduces potential issues, since a new field added would still pass the test, while it should fail the assertion."*
- **`assertEquals` over `assertTrue`** when comparing objects — assertEquals shows the diff on failure.
- **Don't unwrap `Optional` in assertions** — assert on the optional itself.
- **Test the disabled / negative case**: `enabled=false` → output reflects disabled. Reviewers explicitly look for this.

## Mocking discipline

- **Mock external dependencies only**: gRPC clients, stores, external services. Don't mock the class under test or its pure-data inputs.
- **Real protos in tests, not mocks.** Build proto messages with `.newBuilder()` and pass them as inputs.
- **`@InjectMocks`** is the standard pattern for unit tests in this repo — readable signals about what's mocked vs injected.

## Coverage

- **Unit test required for every new service implementation, manager, validator, converter, util.** *"plz add a test for this"*, *"unit test for this class?"* are recurring asks.
- **Integration test for new RPCs that touch the store** — exercises the full path including serialization, store read/write, and gRPC.
- **Test factory routing**: when a new converter/handler is added to a factory, integration-style test verifying the factory routes correctly to it for each action type.

## Lombok in tests

- Mixed opinions; default to plain constructors with `@InjectMocks` for clarity.

## Don'ts

- **Don't test Lombok-generated code** (getters/setters/builders).
- **Don't replicate business logic in tests** — use hardcoded expected values.
- **Don't use lenient mocking** — if you have unused stubs, fix the test.
- **Don't skip / disable / comment out tests** — fix them or remove.
- **Don't change default values across many test files when adding a new field.** *"why do we need these changes in multiple test files? lets define default fallbacks in config class instead"* — if a new config field needs a default for tests, put it in the config class (or a test fixture), not via copy-paste in every test file.

## How to apply during review

- **P0** for missing tests on a new service/validator/converter.
- **P2** for assertions that piecemeal-check fields instead of the full object.
- **P2** for missing negative-case / disabled-case tests.
- **P3** for unit tests that construct real instances of multiple non-data classes (should mock or use Guice).
- **P4** for tests using Lombok where the team prefers plain constructors.

**Why it matters for review:** Test gaps are routinely flagged but rarely block merges in this repo. The skill should call them out so the reviewer can decide.

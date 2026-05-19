---
name: lombok-guice-patterns
type: domain-knowledge
---

## Lombok

- **`@Value` already implies `private final` and `@Getter`**. Don't add either redundantly. *"`private final` is already implied when using `@Value`. Similarly using `@Getter` is also not needed."*
- **`@Value(staticConstructor = "of")`** is the established pattern for factory-style immutables.
- **`@Slf4j` without `log.*` calls** → remove the annotation.
- **Lombok in tests**: opinion is split. Some reviewers prefer plain constructors in tests for clarity (`@InjectMocks` reads more clearly than Lombok-generated injection in tests). Default to plain constructors in tests unless the team file shows Lombok usage.
- **`@Singleton` belongs on classes**, not on `@Provides` methods (when the class itself is constructable). Reviewers ask: *"I think we should annotate these with `@Singleton` too"* — but the canonical place is the class.
- **Don't use static instance / `getInstance()`** patterns. *"We have Guice. We can just directly annotate the class as `@Singleton` to make it a singleton class. We don't need fancy business of getting a static instance."*

## Guice

- **Constructor injection only.** `@Inject` on the constructor, never on fields. (Already in repo style guide; reviewers also enforce on review.)
- **`@Singleton`** on services, managers, stores, validators. Required — without it, every injection creates a new instance.
- **Don't bind interface to implementation when the implementation is the only one.** *"Why not just inject by name rather than interface? Not sure what the point of binding an interface to an implementation if we want to use the implementation."* — flagged when interface adds no value.
- **Multibinder over reflection** for plug-in style registries.
- **Don't `new` injectable types inside modules.** Provide them via `@Provides` or class-level injection.
- **Module install order**: when modules contribute to the same multibinder, order matters. Reviewers check this for new factory registrations.
- **Pass channels, not stubs**, to child Guice modules where possible — lets the child module choose stub style.

## How to apply during review

- **P3** for `@Getter` or `private final` redundant with `@Value`.
- **P3** for missing `@Singleton` on a service/manager/store class.
- **P3** for static `getInstance()` patterns when Guice would do.
- **P3** for binding an interface to its single implementation with no abstraction value.
- **P2** for field injection (already disallowed by repo style guide).

**Why it matters for review:** These conventions are well-established and consistently enforced. Catching them on the diff shortens review cycles for the human reviewer.

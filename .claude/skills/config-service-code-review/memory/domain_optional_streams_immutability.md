---
name: optional-streams-immutability
type: domain-knowledge
---

These are the most-frequent style-but-also-correctness items — reviewers hold the line on these strongly.

## `Optional`

- **`Optional.get()` is a red flag.** *"If you find yourself using `.get()`, it's a red flag. Optionals work much better with functional style coding, but even when using imperative, prefer `.orElseThrow()`."* — explicit reviewer quote.
- **`Optional.ifPresent` over `if + .get()`** in imperative code.
- **Stream-style `Optional`**: if the value flows into a stream/collection, prefer `.stream()` or `.map()` rather than unwrapping then re-wrapping.
- **`Optional` in tests is bad.** *"Unwrapping optionals just makes test failures more difficult to decipher since you get a no value present exception rather than an assertion failure saying expected X, got Y."* Assert on the full object equality.
- **Don't use `Optional` as a substitute for proper error propagation.** *"If something failed at the manager, I'd expect it to throw. Optional means no value, but it shouldn't replace propagating an error."*

## Streams

- **Prefer `stream/map/forEach` over `for` + `if + continue`**. *"stream/map/foreach (not just a style preference, it's less error prone and more readable)."*
- **Avoid `continue`** — it's flagged as a minor antipattern alongside `Optional.get`.
- **Default to `Collectors.toUnmodifiableList()` / `toUnmodifiableSet()`** for collected results. Reviewers consistently flag mutable collections in responses where immutable is appropriate.
- **Don't mutate in a `forEach`** — use `map` + `collect`, or `reduce`. Mutation makes the function impure and harder to reason about.
- **Stream `reduce`** is endorsed for collapsing collections — reviewers point to it as the elegant alternative to manual iteration.

## Immutability

- **Config data should be immutable.** *"Prefer immutable data structures for config."* — recurring reviewer phrasing.
- **Don't mix mutable and immutable** in the same response object. Pick one.
- **Sort methods should not assume mutability.** Either ensure mutability inside the sort, or use a non-mutating sort (`stream.sorted` or Guava `Ordering`).
- **`@Value` (Lombok)** implies `private final` — don't add `private final` redundantly. Don't add `@Getter` on a `@Value` class — it's already implied.

## Switch / branching

- **Avoid `switch` with mutation breaks.** *"We should directly return a map constructed inside the case statement. Avoids the breaks (error prone) and mutability."* — prefer expression-style return per case.
- **`default` case in switch is required** — even when only for "log error and skip". Used to catch new enum values added later. *"The `default` case is mostly needed for error logging or throwing an exception, so that we can catch early if there is a new type added in the API."*

## Map building

- **Don't mutate a map up front, then return it** — use `Map.of(...)`, `ImmutableMap.builder()`, or build the map inside the case statement.
- **Don't inject `Map<K, V>` directly** when the lifecycle should be longer-lived than module construction — inject a fetcher/provider class instead.

## How to apply during review

- **P3** for any `Optional.get()`, `for + continue`, mutable collection in response, mutating switch.
- **P4** for redundant `@Getter` on `@Value`, redundant `private final` in `@Value`.
- **P2** if `Optional` is used to swallow a real error.

**Why it matters for review:** These add up — reviewers raise many small items here per PR. Catching them up front saves review-cycle latency.

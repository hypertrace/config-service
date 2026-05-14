# Code Style

- **Use Lombok annotations** for boilerplate reduction: `@Slf4j`, `@Inject`, `@Singleton` (for Guice components)
- **Constructor injection preferred** over field injection: use `@Inject` on constructor, not on fields
- **Make variables `final`** wherever possible, including local variables and exception bindings
- **Add comments only for "Why"**, not "What" or "How" — code should be self-documenting
- **NEVER use Fully Qualified Class Names (FQCNs)** — always add imports and use short names (exception: conflicting class names only)
- **Lambda parameters must be meaningful** — use descriptive names (e.g., `config`, `request`, `entry`), never single-character names
- **Use `log.isDebugEnabled()` guards** before `log.debug()` calls in hot paths
- **Proto builders**: always call `.build()` — never pass `Builder` instances across method boundaries
- **Guice injection**: extend `AbstractModule` for modules, use constructor injection with `@Inject` and `@Singleton` — avoid field injection
- **Follow Hypertrace codestyle conventions** as enforced by the Hypertrace codestyle Gradle plugin

## Don'ts

- **Don't commit** build output files (e.g., compiled classes, JARs)
- **Don't skip** `./gradlew spotlessApply build` before committing
- **Don't add dependencies** without discussion — OWASP dependency check will fail on CVSS >= 7.0
- **Don't modify proto files** without running `buf breaking` to check for breaking changes
- **Don't use field injection** — always use constructor injection with `@Inject`
- **Don't commit large binary files** or generated code to version control

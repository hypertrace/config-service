---
name: validation-and-error-handling
type: domain-knowledge
---

## Reviewer rules on validation

- **Don't use `Preconditions.*`** — *"Avoid using preconditions — throw GRPC exceptions with appropriate status codes."* Always throw `Status.INVALID_ARGUMENT.withDescription(...).asRuntimeException()` (or similar) for input validation failures so the gRPC status code is correct.
- **One validator, called at one place.** *"I would rather suggest to keep a single validator for all the service requests."* — Don't duplicate validation across the service entry, manager, and store. Centralize in a single `*RequestValidator` injected via Guice.
- **Validate `RequestContext` (tenant ID) explicitly** even if the request body is mostly optional fields. *"Need to validate the request context too, I'm guessing (for tenant id)."*
- **Don't validate the same thing twice.** If the create/update path will fail naturally (e.g. on insert), don't pre-check via a DB read — just catch `NOT_FOUND` on delete, etc. *"No real reason to check first — just catch a not_found on delete."*
- **Validation errors log at WARN, not ERROR.** *"Prefer warning log messages here — a request that fails validation should not log error. Error is for: something happened that requires on call intervention."*
- **`validateNonDefaultPresenceOrThrow`** is the established helper for "field must be set" — reuse it instead of writing custom checks.

## Reviewer rules on error handling

- **Use the right gRPC status code.** `INVALID_ARGUMENT` for bad input, `NOT_FOUND` for missing entity, `INTERNAL` for unexpected, `ALREADY_EXISTS` for create conflicts. Reviewers will flag wrong codes.
- **Decorate exceptions before sending them back** so the caller gets correlation info — request ID is in trailers, but a clear message helps the caller log.
- **Match exception messages to the actual problem.** *"Please update the message to match the exception."* — generic "internal error" messages are flagged.
- **Don't swallow exceptions silently.** If you `try/catch` and translate to `Optional.empty()`, that's a deliberate design choice — reviewers ask whether the silent path needs at least a warn log.
- **No stack traces in exception messages sent to the agent** — the agent doesn't need internal details. Log the stack trace server-side, return a clean message.
- **Default-on-error vs throw**: default-on-error (e.g. cache loader returns default) is dangerous because the cache may store the default permanently. Reviewers prefer throwing → cache rejects load → next request retries. *"So we'd let this fetch method throw, which should prevent the cache from accepting the reload."*
- **`@SneakyThrows`** is acceptable when wrapping a converter that always rethrows — reviewers explicitly endorse this over wrap/unwrap noise.

## Reviewer rules on logging

- `@Slf4j` present without `log.*` calls → remove the annotation.
- Wrap `log.debug` in `log.isDebugEnabled()` in hot paths.
- Log on silent failure paths (empty result, missing tenant, validation rejection).
- Include `tenant_id` and a place identifier (e.g. "Error creating X for tenant {}").
- Never log credentials, tokens, full config payloads with sensitive data.

## How to apply during review

- **P0** if validation errors propagate as `INTERNAL` instead of `INVALID_ARGUMENT`.
- **P0** if `Preconditions.*` is used in a gRPC handler path.
- **P2** if a silent failure path has no log statement.
- **P2** if validation is duplicated across service + manager + store.
- **P4** if `@Slf4j` is present but unused.

## See also

- [[grpc-handler-pattern]] — the canonical `try/catch + log(request, requestContext) + onError(e)` shape every handler is expected to follow.

**Why it matters for review:** Wrong status codes break clients' retry/error logic. Silent failure paths cause hours of debugging. These are the most common P2 patterns the reviewers raise.

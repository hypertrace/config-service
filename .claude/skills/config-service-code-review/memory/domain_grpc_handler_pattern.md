---
name: grpc-handler-pattern
type: domain-knowledge
---

The canonical gRPC handler shape in this repo, called out explicitly by reviewers as "our pattern":

```java
public void someRpc(
    Request request,
    StreamObserver<Response> responseObserver) {
  RequestContext requestContext = RequestContext.CURRENT.get();
  try {
    someValidationMethod();                      // throws on bad input
    responseObserver.onNext(mainBusinessMethod()); // builds response
    responseObserver.onCompleted();
  } catch (Exception e) {
    log.error(
        "Failed to <action> for request: {} within context: {}",
        request,
        requestContext,
        e);
    responseObserver.onError(e);
  }
}
```

## Key elements reviewers expect

- **`RequestContext requestContext = RequestContext.CURRENT.get();`** at the top of the handler — read it once, pass it down explicitly to managers/stores.
- **Single broad `try/catch`** wrapping the whole handler body — not per-step. Wrapping `validation`, `manager call`, and `responseObserver.onNext` together keeps the error path uniform.
- **Log** with both `request` and `requestContext` (full RC, not just tenant ID) — this is what's used for debugging in production. *"log request context too"*, *"log complete requestContext instead"*.
- **`responseObserver.onError(e)`** — propagate the exception. Don't translate to `INTERNAL` here; let the gRPC framework convert based on the exception type. Validators throw `Status.INVALID_ARGUMENT.asRuntimeException()`, stores throw `Status.NOT_FOUND.asRuntimeException()`, and they propagate as-is.
- **No silent `Optional.empty()` for what should be errors.** If the manager returns optional and empty means "not found", the handler converts to `NOT_FOUND` explicitly.

## Anti-patterns flagged in recent PRs

- **Per-step try/catch with detailed re-throws** — adds noise without value. The broad pattern above is preferred. *"the earlier catch block was correct, we follow this pattern."*
- **Logging only tenant ID instead of full request context** — RC carries more (auth, headers, request ID) which is needed to debug.
- **Calling `RequestContext.CURRENT.get()` deep in the manager** instead of passing it from the handler.

## How to apply during review

- **P2** if a new handler doesn't follow this exact shape (top-level `RequestContext.CURRENT.get()`, single try/catch, log both `request` and `requestContext`, `onError(e)`).
- **P2** if errors log only the tenant ID instead of the full request context.
- **P3** if try/catch is split per-step without good reason.

**Why it matters for review:** This is the de facto repo idiom — reviewers explicitly named it as "our pattern". Diverging from it makes a PR harder to review and harder to debug in production.

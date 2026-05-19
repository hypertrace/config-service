---
name: tenant-scoping-request-context
type: domain-knowledge
---

Tenant scoping bugs are treated as P0 in this repo. Reviewers consistently push back on three classes of mistake.

## Rules the reviewers enforce

1. **Pass `RequestContext` explicitly into every manager / store / utility method.** Don't rely on `RequestContext.CURRENT.get()` deep in the call stack — fetch once at the gRPC entry point and pass it through. Reviewers' phrasing: *"all manager calls should pass the grpc request context. We're relying on the fact the call will be received and the next call will be executed all in the same thread, which is an easy source of errors."*
2. **Never put `tenant_id` in a proto.** Tenant always comes from `RequestContext`. *"We should never have tenant ids in the protos."*
3. **All async/threaded work and remote calls must be wrapped in the request context.** Bare gRPC stub calls (without `GrpcClientRequestContextUtil.executeWithHeadersContext` or `Context.call`) are flagged as bugs. *"Remote calls are typically wrapped in a request context, these ones are bare and thus relying on the thread's context."*

## Cache key implications

- Caches keyed by tenant alone are too coarse for user-scoped data. Use `ContextualKey` with `buildUserContextualKey` (looks at auth) for user-scoped, `buildInternalContextualKey` (only tenant) for ingestion/background paths.
- Cache key is the **most restrictive context** that affects the result, not the most permissive. Service-scope caches commonly use the full `RequestContext` as the key.

## Logging

- Log `tenant_id` (and ideally request ID) on errors — reviewers ask for this repeatedly. *"log tenant id and the place — say Error in creating etc."*
- Don't log raw config payloads if they may contain credentials/tokens.

## How to apply during review

If a PR adds a new RPC, manager method, store call, or async task:
- **P0 finding** if `RequestContext` is fetched from `CURRENT.get()` deep in the stack instead of being passed in
- **P0 finding** if a new proto contains `tenant_id` (or equivalent like `customer_id`) — should come from context
- **P0 finding** if a stub call inside the impl is bare (not wrapped in a context call)
- **P2 finding** if cache uses tenant alone where user-scoped data is involved
- **P2 finding** if errors don't log `tenant_id`

**Why it matters for review:** This is one of the most common P0 classes the reviewers raise. Cross-tenant data leakage and silent context loss are the two failure modes; both are catchable from the diff alone.

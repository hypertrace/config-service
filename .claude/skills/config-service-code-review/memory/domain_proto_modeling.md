---
name: proto-modeling
type: domain-knowledge
---

Proto modeling is the second-most-discussed topic. The reviewers have strong, repeated preferences. The skill should call out anything that violates these on sight.

## `oneof` vs enum vs optional vs separate fields

- **`optional` is syntax sugar for a one-member `oneof`**. If a field might gain new sibling cases later, prefer `oneof` from the start.
- **An enum + matching optional string is duplicative.** If the string's presence already implies the enum value, drop the enum.
- **`oneof` for exclusive cases, repeated lists for additive cases.** Reviewers ask: "are these mutually exclusive (oneof) or can multiple coexist (separate fields/lists)?"
- **Don't pair an enum with a `oneof`** that already generates an enum — pick one. Reviewers will flag this as duplicative and a source of inconsistency.
- **Implicit OR / implicit AND** between repeated lists is a common debate — they prefer making the semantic explicit (oneof or a clear logical predicate type), not an implicit assumption that consumers must learn.

## Defaults

- **Enum 0 must be `*_UNSPECIFIED`** (or equivalently the safe/sentinel default). Don't make a meaningful value the 0 case.
- **proto3 has no `required`**. Don't pretend it does — make impl tolerant of missing fields, OR explicitly validate with a clear gRPC `INVALID_ARGUMENT`.
- **Don't set defaults as protection-mode-style "default" values in the API surface** unless there's a real product use case. "Default usually means: if config not present, return default" — don't conflate "a default mode" with "absence of config".
- **`reserved` is required** when you remove a field or enum value, to prevent number reuse.

## Breaking changes

- **Field renumbering, type changes, or removing a field/enum value without `reserved` = breaking.** Reviewers ask: "Is this consumed by an agent or another service? If yes, this is breaking and needs a version gate / FF / migration."
- **Renaming a proto message is generally NOT a wire-breaking change** (numbers are what's serialized) — but renaming a field IS, since field names appear in JSON. Verify by use case.
- **Changing a field type on an in-use field = breaking**, even if the new type is "compatible enough".
- **Deprecating a field**: mark `[deprecated = true]`, add a comment pointing to the replacement, keep the field readable from old data. Don't remove until all readers have migrated.

## Proto structure

- **Don't put `tenant_id` in protos** — it comes from `RequestContext`. Storing it in the message is a smell.
- **Avoid `repeated` when the use case is "one root"** (e.g. tree structures with a single root) — use a single message.
- **`StringList` wrapper vs `repeated string`** — be consistent with neighboring scopes; switching styles mid-message draws pushback.
- **Top-level vs nested fields**: scope/filter fields tend to live at the same level as the data they scope. Don't nest a filter inside another filter without reason.
- **Oneof field placement**: reviewers prefer the `oneof` to come early in the message and not be split by unrelated fields.
- **Field number gaps**: leaving gaps between fields to "reserve room" for future related fields is acceptable and explicitly endorsed.
- **`GeneratedMessageV3`** is the right import for generic proto handling — flagged when wrong base used.

## Common reviewer questions on proto changes

- "Is this consumed by the agent? Is it a breaking change for them?"
- "Why is this an enum and a oneof? They're duplicative."
- "Why repeated? Is this expected to support multiple, or is there always one?"
- "Should this be `optional`, or wrap in a `oneof` for future extension?"
- "Does an `*_UNSPECIFIED = 0` exist?"
- "Where's the deprecation comment / migration path?"

## Recent recurring sub-patterns

- **Split create vs update message shapes.** A `Create*Request` should not include the id (server generates); an `Update*Request` requires the id. Reviewers ask: *"create will not have an id so can't be `EntityDerivationConfig`. Split `EntityDerivationConfig` into id and `EntityDerivationConfigData`?"* — the canonical split is `*Config` (with id) wrapping `*ConfigData` (without id).
- **Use `Get` semantics, not custom verbs, for reads.** Reviewers consistently push `Get*Request` over creative naming. *"Should be Get request"*, *"Please use Get"*, *"use Getter for this"*.
- **Naming reads when no filters are present**: `GetAll*ConfigsRequest` is preferred when the request body is empty or fully optional. If filtering is added later, rename or accept filters in the same request rather than a new RPC.
- **`buf.yml` registration**: new `*-api` modules must be added to `buf.yml` so proto compatibility validation runs in CI. *"You may have to add this module to `buf.yml` for proto. compatibility validation."*
- **Type-change exemption for unused APIs**: if an RPC isn't yet consumed, breaking changes can be allowed via platform-team exemption — but flag it explicitly. *"This should be fine since no one is using the API yet. So we can get an exemption from this type change enforcement."*
- **"type" + "kind" together is a smell.** *"Need to rename this. Can't have both type and kind"* — pick one term and use it consistently.
- **`oneof` for system-defined extensible kinds.** When modeling an extensible category (e.g. `SystemEventKind`, transformation function types), reviewers prefer a `oneof` over an enum — easier to extend with heterogeneous parameters per case. *"a simple one of shows the intent more clearly"*.

**Why it matters for review:** Proto shape decisions are the hardest to undo — they're versioned, consumed by the agent, and stored on disk. Catching the wrong shape during review is the entire point.

# GraphQL Interpreter — Implementation / Design Document

Status: **DRAFT for review** — see the [Open Questions](#open-questions) at the end.

### Decisions locked (2026-06-20)
- **Scope:** POC covers branching, not just the linear path — wire
  `window` / `after` / `neighbours` so `test_graph_windows_and_layers_query`
  runs and the tree/nested-list machinery is exercised from day one. (Q6)
- **Engine selection:** Rust differential harness first, no HTTP surface yet —
  run both `Schema::execute` and `Interpreter::run` on the same graph and assert
  identical JSON. (Q1)
- **Validation:** load `schema.graphql` into an `async_graphql::dynamic::Schema`
  at startup and reuse its validator; `schema.graphql` stays authoritative. (Q2)
- **Errors:** simple model for the POC — valid query succeeds, invalid query
  returns a single error; defer exact path/locations and non-null
  error-bubbling parity. (Q4)

- **`Value::Str`:** owned `String` for now. (Q5)
- **Sink = streaming `io::Write` over a channel (Q3/Q7).** Raphtory interaction
  runs on the rayon `COMPUTE_POOL`; the `Sink` batches bytes into a buffer and,
  every ~4Kb, ships an owned `Vec<u8>` chunk over a channel. Poem drains the
  channel and flushes each chunk straight to the HTTP response — **the full
  response is never concatenated / collected.** See [§6](#6-the-streaming-sink).
  Crucial corollary: execution is **spawned** on the compute pool (not awaited
  via `blocking_compute`, which would deadlock — the channel would fill while the
  consumer waits for completion). Poem returns the streaming body immediately and
  draining happens concurrently with production.
- **Channel:** bounded `tokio::sync::mpsc<Vec<u8>>`, capacity ≈ 8 chunks
  (≈32Kb in flight). Producer uses `blocking_send` on the rayon thread
  (backpressure blocks the worker); async consumer `recv`s into the response
  stream. (Q-A)
- **Buffers:** fresh `Vec<u8>` per chunk for the POC; revisit a recycling pool
  only if profiling demands it. (Q-B)
- **Mid-stream errors:** the POC subset is effectively infallible once streaming
  starts; an "impossible" mid-stream error **aborts the stream** (drop sender →
  truncated body) rather than injecting a late error object. Clean
  `{"errors":…}` is still produced for validation/graph-load failures, which
  happen before the first flush. (Q-C)
- **Harness:** the differential test may drain + concatenate chunks *in the test*
  to compare against async-graphql; the engine/production path never
  concatenates. (Q-D)

### Implementation status

| Component | State |
|-----------|-------|
| `sink.rs` — streaming `Sink` + bounded-channel → poem `Body` plumbing | ✅ done, tested (unit + HTTP e2e) |
| Poem POC route `GET /graphql_stream_poc` | ✅ proves producer→channel→response streaming |
| `value.rs` — `Value` enum | ✅ `Graph` / `Node` / `History` / `EventTime` |
| `plan.rs` — `Plan` / `Op` / `Nav` / `IterKind` / `LeafKind` | ✅ generic tree (branching-ready) |
| `exec.rs` — depth-first executor over the stack | ✅ `nodes→list→id` and `node→history→list→{timestamp,eventId}` |
| `schema.rs` — SDL type map (validation source) | ✅ parses `schema.graphql` once; `(type,field)→return-type` |
| `planner.rs` — parse + validate + AST→Plan | ✅ type-directed walk; rejects unknown & unimplemented fields |
| **Full vertical slice** `graph{nodes{list{id}}}` request→validate→plan→execute | ✅ `vertical_slice_matches_endpoint` — byte-identical to live endpoint |
| Differential test vs live async-graphql endpoint | ✅ `matches_async_graphql_endpoint` + `vertical_slice_matches_endpoint` + branching query over HTTP |
| Branching navs (`window` / `after` / `before` / `neighbours`) | ✅ wired through planner + exec; `Value::Path` added; full branching query (window→node→after→{history, neighbours→list→{name, before→history}}) differential-tested over HTTP |
| Edges: `graph.edge(src,dst)` / `graph.edges` / `edges.list` | ✅ `Value::Edge`/`Value::Edges`; `edge.history`/`src`/`dst`/`id`/`window`/`after`/`before`/`layer`; `layer(name:)` (Graph/Node/Edge); differential-tested over HTTP (incl. windowed/layered/null edge) |
| Properties block: `node/edge.properties` + `metadata`, `values(keys:)`, `temporal`, `Property.{key,asString,value}`, `TemporalProperty.{key,history}` | ✅ `Value::Properties/TemporalProperties/Metadata/Property/TemporalProperty`; `IterKind::*Values(Option<keys>)`; `value` serialized via `prop_to_gql` for exact parity; differential-tested over HTTP (string/int/float values, with/without `keys`) |
| Edge/node filtering (`edges(select:)`, `filterEdges`, `neighbours(select:)`, property `where` expressions) | ⬜ rejected as `Unsupported` for now (would change output) |
| History sub-objects (`timestamps`/`datetimes`/`eventId` lists, `eventTime.datetime`) | ⬜ only `history.list.{timestamp,eventId}` so far; `test_gql_history.py` shapes need these next |

> **Validation note:** async-graphql's validator (`check_rules`) is `pub(crate)`,
> so the locked Q2 ("reuse async-graphql's validator") isn't reachable. We instead
> parse `schema.graphql` into a type map and validate during the planning walk —
> `schema.graphql` stays the single authoritative source, which was the intent.

### HTTP interop (`http.rs`)

The interpreter is mounted as its own poem endpoint at **`POST /graphql_interp`**,
alongside the async-graphql endpoint at `/`. Wired in `server.rs` with a clone of
`Data`. Flow: read body → `plan_request` (parse + SDL-validate) → load graph via
`Data::get_graph_unfiltered` (the only async step) → `streaming_body(execute(...))`.
Tested e2e (`interp_endpoint_matches_async_graphql`) — byte-identical to `/`.

**Why not an async-graphql `Extension` / middleware** (investigated):

| Approach | Verdict |
|----------|---------|
| `Extension::execute` hook (intercept post-validation) | ✗ returns a **materialised** `Response`; `ExtensionContext` doesn't expose the parsed document → can't stream and can't get the AST out |
| Standalone reuse of `check_rules` | ✗ `pub(crate)`; `prepare_request` also `pub(crate)`; static-schema `Registry` not exposed |
| Dynamic `Schema` from SDL as a pure validator | ✗ no validate-only call; `.execute()` runs resolvers — can't separate validation errors from "unimplemented field" errors |
| **Dedicated poem endpoint** (chosen) | ✓ full raw-byte streaming, full control; validation via our SDL walk (authoritative against `schema.graphql`) |

Raw-byte streaming can only happen where poem owns the response body, so the
endpoint layer is the correct seam regardless. If async-graphql later exposes
`check_rules`/`Registry` publicly, we can swap in their validator behind
`plan_request` without touching the HTTP layer.

**Not yet wired:** auth/permissions on `/graphql_interp` (the async-graphql path
still enforces them via `AuthenticatedGraphQL`); `get_graph_unfiltered` skips
row-level filtering. To address before this is more than a POC.

This document turns the sketch in [`graphql-interpreter.md`](./graphql-interpreter.md)
into a concrete design for a push-based, (near-)zero-allocation GraphQL execution
engine that lives **alongside** the existing `async-graphql` / `dynamic-graphql`
resolver stack and is verified against it.

---

## 1. Goals & non-goals

### Goals
- Take a raw GraphQL request and produce **byte-identical JSON** to the current
  endpoint for the supported subset.
- Compile a request into an **execution plan** once, then execute it by pushing
  values onto an explicit stack and writing leaves directly into an output sink.
- **No per-result heap allocation** during execution: no `Vec<GqlEventTime>`, no
  `serde_json::Value` trees, no intermediate `GqlNode`/`GqlHistory` wrappers. The
  only allocations are (a) the plan, built once, and (b) the stack, which grows
  to the maximum query depth and is reused.
- **Pre-resolve** every field name, argument, and receiver position at plan time
  so execution does zero string lookups and zero dynamic name matching.
- **Validate** the request against [`schema.graphql`](./schema.graphql) *before*
  building the plan, so the planner only ever sees well-typed documents.

### Non-goals (for the POC)
- Mutations, subscriptions, fragments, variables, directives, introspection.
- Full schema coverage. The POC implements exactly one path:
  `graph → node → history → list → { timestamp, eventId }`.
- Replacing the existing engine. This is additive and opt-in.

---

## 2. Where it plugs in

Today a request flows:

```
poem POST /  →  AuthenticatedGraphQL::call (src/auth.rs)
             →  async-graphql Schema::execute  (built from App in src/model/mod.rs)
             →  ResolvedObjectFields resolvers (GqlGraph, GqlNode, GqlHistory, …)
             →  serde JSON response
```

The new engine slots in as an **alternate executor** reachable from the same
poem handler. Auth, graph loading (`Data::get_graph_with_read_permission`), and
the `Data` context are reused unchanged — we only replace the
*parse → resolve → serialize* middle.

```
poem POST  →  auth (unchanged)  →  ┌─ default: async-graphql Schema::execute
                                   └─ opt-in: Interpreter::run  ← NEW
```

Both paths share `Data`, so the same graph cache and permission checks apply.
See [Open Questions Q1](#q1-how-do-we-select-the-interpreter) for how a request
opts in.

---

## 3. Pipeline overview

```
 raw query string
        │
        ▼
 ┌─────────────┐   async-graphql-parser (already in tree)
 │   PARSE     │   → ExecutableDocument (AST)
 └─────────────┘
        │
        ▼
 ┌─────────────┐   validate AST against schema.graphql
 │  VALIDATE   │   → reject unknown fields/args/types before planning
 └─────────────┘
        │
        ▼
 ┌─────────────┐   walk the validated selection set top-down,
 │   PLAN      │   resolving each field to a typed Op with parsed args
 └─────────────┘   → Plan (tree of Ops, no strings, no Strings)
        │
        ▼
 ┌─────────────┐   push/pop typed Values on a stack;
 │  EXECUTE    │   leaves write straight into the sink
 └─────────────┘   → bytes
        │
        ▼
 impl Write sink  →  { "data": { … } }
```

---

## 4. The `Value` enum

`Value` is the runtime payload pushed on the stack. It holds the *typed Raphtory
receivers* for navigation steps plus scalar leaves for emission. It is `enum`,
not `Box<dyn>`, so it lives on the stack with no per-value allocation. The
heavyweight variants are already cheap to move/clone (Arc-backed views).

```rust
enum Value {
    // ── navigation receivers (POC) ──
    Graph(DynamicGraph),                              // from graph(path:)
    Node(NodeView<'static, DynamicGraph>),            // from node(name:), after(time:), …
    History(History<'static, Arc<dyn InternalHistoryOps>>),
    EventTime(EventTime),                             // a single history entry (the list item)

    // ── scalar leaves (written to the sink) ──
    Int(i64),
    OptInt(Option<i64>),    // timestamp / eventId are nullable in the schema
    Str(/* small inline or Arc<str> */),
    Bool(bool),
    Null,
}
```

Notes:
- `DynamicGraph`, `NodeView`, and `History` are all Arc-backed handles — moving
  them between stack slots is a pointer copy / refcount bump, not a data copy.
  This is the same handle the existing `GqlGraph`/`GqlNode`/`GqlHistory` wrap, so
  output parity is structural, not coincidental.
- `EventTime` is `EventTime(i64, usize)` — a 16-byte `Copy` value. The `timestamp`
  and `eventId` leaves read `.t()` and `.i()` off it directly.
- Future variants (`Edge`, `Edges`, `Nodes`, `PathFromNode`, `Properties`, …)
  extend the enum without changing the execution model.

---

## 5. Plan representation & the execution stack

### 5.1 The query is a *tree*, not a line

The sketch in `graphql-interpreter.md` shows a single linear stack, but real
queries branch — e.g. `after(...)` below feeds **both** `history` and
`neighbours`, and `neighbours.list` items each emit `name` **and** `before`:

```
after
 ├─ history → list → { timestamp, eventId }
 └─ neighbours → list → { name, before → history → list → { … } }
```

So the plan is a **tree of `Op`s**, and the stack is the *environment* during a
depth-first walk: descending a navigation step pushes a `Value`; finishing that
subtree pops it. A branch point (an object with several sub-fields) runs each
child against the same top-of-stack receiver.

### 5.2 Op kinds

Each plan node carries its **response key** (the output JSON key — alias or field
name) and one of:

```rust
enum Op {
    /// Produce one new receiver from an ancestor on the stack, push it,
    /// run `children` as a JSON object, then pop. (graph, node, after, window, history)
    /// `nav` is a pre-resolved function pointer / enum variant — no name lookup.
    Navigate { key: Key, nav: Nav, args: Args, nullable: bool, children: Box<[Op]> },

    /// Take an iterable receiver from top-of-stack, emit a JSON array; for each
    /// item push it, run `children` as an object, pop. (history.list, nodes.list)
    List { key: Key, iter: IterKind, children: Box<[Op]> },

    /// Read a scalar from the receiver at a pre-resolved stack position and
    /// write it straight to the sink. (timestamp, eventId, name)
    Leaf { key: Key, leaf: LeafKind, input: StackSlot },
}
```

- `Nav`, `IterKind`, `LeafKind` are **enums of pre-resolved operations** (one
  variant per supported field), not strings. Dispatch is a `match`, which the
  compiler lowers to a jump table.
- `Args` are **parsed once** at plan time into typed values (e.g. `after(time:500)`
  → `EventTime`/`i64`; `node(name:"Frodo")` → an owned `GqlNodeId`). No re-parsing
  of `async_graphql::Value` during execution.
- `StackSlot` is the pre-resolved index of the receiver a leaf reads from
  (these are the “pre-resolved bindings and positions” from the brief). In
  practice most leaves read top-of-stack, but storing the slot makes ancestor
  access (e.g. a leaf that needs the enclosing node) explicit and lookup-free.
- `children: Box<[Op]>` — the plan is built once; the slices are immutable during
  execution.

### 5.3 Execution loop

```rust
fn exec(op: &Op, stack: &mut Vec<Value>, out: &mut Sink) {
    match op {
        Op::Navigate { key, nav, args, nullable, children } => {
            let recv = stack.last().unwrap();
            match nav.apply(recv, args) {           // typed match → produce Value
                Some(v) => {
                    out.begin_field(key); out.begin_object();
                    stack.push(v);
                    for c in children { exec(c, stack, out); }
                    stack.pop();
                    out.end_object();
                }
                None if *nullable => out.field_null(key),
                None => { /* surface as GraphQL error */ }
            }
        }
        Op::List { key, iter, children } => {
            out.begin_field(key); out.begin_array();
            for item in iter.make(stack.last().unwrap()) {   // lazy iterator, no collect
                out.begin_object();
                stack.push(item);
                for c in children { exec(c, stack, out); }
                stack.pop();
                out.end_object();
            }
            out.end_array();
        }
        Op::Leaf { key, leaf, input } => {
            leaf.write(&stack[*input], key, out);   // read + serialize in place
        }
    }
}
```

The recursion depth equals the query nesting depth (already capped by
`schema.max_query_depth`). The `stack` `Vec<Value>` is the only growable
structure and is reused for the whole request.

---

## 6. The streaming Sink

`Sink` is the output writer. It is **not** an in-memory buffer of the whole
response — it batches bytes into a small buffer and ships fixed-size chunks over
a channel to poem, which flushes each chunk to the HTTP response as it arrives.
The full response is never materialized in memory.

### 6.1 The producer/consumer split

```
        rayon COMPUTE_POOL thread                    tokio / poem
   ┌───────────────────────────────┐         ┌──────────────────────────┐
   │ exec(plan) ─► Sink::write_*    │  chunk  │ Body::from_bytes_stream   │
   │   buffer (Vec<u8>, cap ~4Kb)   │ ──────► │  for each Vec<u8>: flush  │
   │   on ≥4Kb: send(buffer), reset │ channel │  to HTTP response (no     │
   │   on finish: flush remainder   │         │  concatenation)           │
   └───────────────────────────────┘         └──────────────────────────┘
```

- The `Sink` owns: a `buf: Vec<u8>` (capacity ≈ 4096) and the channel **sender**.
- `write(&[u8])` copies the slice into `buf`. When `buf.len() >= 4096`, it
  `mem::take`s the buffer, sends the owned `Vec<u8>` over the channel, and starts
  a fresh buffer.
- `flush()` sends whatever remains in `buf` (the final partial chunk).
- Poem holds the **receiver**, adapts it to a `Stream<Item = Result<Bytes>>`, and
  hands it to `Body::from_bytes_stream` — exactly the shape already used in
  `auth.rs`. Each received chunk is written to the socket and flushed; chunked
  transfer-encoding falls out naturally.

### 6.2 Why execution is spawned, not awaited

`blocking_compute` returns the closure's result via a oneshot and is `.await`ed —
that would only resolve when execution *finishes*. But the consumer can't drain
the channel until poem has the response body, and poem only gets the body after
the handler returns. Awaiting completion first → channel fills → the rayon thread
blocks on `send` → deadlock. So:

1. Handler `await`s the (genuinely async) graph load.
2. Handler builds the plan + the `(sender, receiver)` channel and the `Sink`.
3. Handler **spawns** `exec(plan, sink)` onto `COMPUTE_POOL` (fire-and-forget).
4. Handler immediately returns `Response { body: stream(receiver) }`.

Production and draining now run concurrently.

### 6.3 Backpressure

A **bounded** channel gives free backpressure: if poem (or the client socket) is
slow, the channel fills and the producer's `send` blocks the rayon thread, capping
memory at `capacity × 4Kb`. The clean fit is `tokio::sync::mpsc` (bounded): the
producer on the rayon thread calls `blocking_send` (blocks the worker, the desired
behaviour), and the async consumer `recv`s straight into the stream. (Single
producer ⇒ effectively SPSC.) See [Q-A](#q-a-channel--backpressure).

### 6.4 The typed API

The interesting part is the typed helpers that emit *valid GraphQL JSON structure*
— they track object/array/comma state so callers never hand-write punctuation:

```rust
impl Sink {
    fn begin_object(&mut self); fn end_object(&mut self);
    fn begin_array(&mut self);  fn end_array(&mut self);
    fn begin_field(&mut self, key: &Key);   // writes (comma?) "key":
    fn field_null(&mut self, key: &Key);
    fn write_i64(&mut self, v: i64);
    fn write_str(&mut self, s: &str);        // JSON-escaped
    fn write_bool(&mut self, b: bool);
    fn finish(self);                         // flush remainder, drop sender
}
```

`io::Write` (`write` + `flush`) is implemented too — handy for `write!`-style
number formatting and raw byte copies — but the typed methods are the primary
surface. (Per the brief: we don't *need* `io::Write`, it just gives us `write`
and `flush` for free.)

### 6.5 Envelope ownership

The `Sink` writes the entire envelope: it opens with `{"data":` before the root
field and closes with `}`. The simple-error path (validation or graph-load failure
*before* any chunk is sent) instead writes a `{"errors":[…],"data":null}` document
— possible only because nothing has been flushed yet. Once streaming has begun the
data shape is committed; see [Q-C](#q-c-mid-stream-errors).

### 6.6 Output-format parity

Parity requirements with async-graphql (critical for differential testing):
- **Key order = selection order.** GraphQL responses preserve the order fields
  appear in the query; our DFS over `children` does this for free.
- **Response key = alias or field name** (the `Key`), not the resolver name.
- The whole payload is wrapped as `{"data": { … }}`; errors use GraphQL's
  `{"errors":[…], "data":null}` shape. See [Q4](#q4-error-semantics).
- Number formatting must match (e.g. integers vs the float-stability concerns in
  `test_float_is_stable_on_roundtrip`) — the POC only emits integers, so this is
  deferred, but the sink should reuse the same number formatting as serde_json.

---

## 7. Validation against `schema.graphql`

The brief requires validating the request against `schema.graphql` before
planning. `schema.graphql` is the SDL already emitted by the build. Proposed
approach (see [Q2](#q2-validation-mechanism) for alternatives):

1. **At startup**, load `schema.graphql` into an `async_graphql::dynamic::Schema`
   (SDL → schema) once and keep it next to the interpreter. This gives us a typed
   schema object whose validator we can run without executing.
2. **Per request**, parse with `async-graphql-parser` → `ExecutableDocument`,
   then run async-graphql's validation pass against the dynamic schema. Reject
   with the standard GraphQL error format on failure.
3. The planner then walks the **validated** document. Because validation has
   already confirmed every field exists on its parent type and every argument is
   well-typed, the planner's field→`Op` resolution is a total function over a
   known-good input (it can `unreachable!()` on truly impossible cases).

This keeps a single source of truth (`schema.graphql`) and means the interpreter
can never be asked to plan a field the schema doesn't define.

---

## 8. Binding pre-resolution

“Pre-resolve bindings and positions to avoid string look-ups” concretely means
the planner produces, per field:

| Query text            | Pre-resolved into                                   |
|-----------------------|-----------------------------------------------------|
| `graph(path:"lotr")`  | `Nav::Graph` + `Args::Graph { path, graph_type }`   |
| `node(name:"Frodo")`  | `Nav::Node` + `Args::Node { id: GqlNodeId }`        |
| `after(time:500)`     | `Nav::After` + `Args::Time(EventTime)`              |
| `history`             | `Nav::History` (no args)                            |
| `list`                | `IterKind::HistoryList`                             |
| `timestamp`           | `Leaf::Timestamp` + `StackSlot(top)`                |
| `eventId`             | `Leaf::EventId` + `StackSlot(top)`                  |

So at execution time there are **no `&str` comparisons**, no `HashMap<String,…>`
lookups, and no `async_graphql::Value` argument decoding — all of that happened
once, at plan time.

---

## 9. POC scope

### Supported query
```graphql
{
  graph(path: "g") {
    node(name: "ben") {
      history {
        list {
          timestamp
          eventId
        }
      }
    }
  }
}
```

### Mapped to the existing resolvers it must match
- `graph(path)`     → `QueryRoot::graph` → `Data::get_graph_with_read_permission` → `GqlGraph`
- `node(name)`      → `GqlGraph::node` → `DynamicGraph::node` → `Option<GqlNode>`
- `history`         → `GqlNode::history` → `NodeView::history()` → `GqlHistory`
- `list`            → `GqlHistory::list` → `history.iter().map(Into::into)`
- `timestamp`       → `GqlEventTime::timestamp` → `EventTime::t()` (nullable `Int`)
- `eventId`         → `GqlEventTime::event_id` → `EventTime::i() as u64` (nullable `Int`)

### Plan produced (illustrative)
```
Navigate{ key:"graph", nav:Graph, args:{path:"g"}, nullable:true, children:[
  Navigate{ key:"node", nav:Node, args:{id:"ben"}, nullable:true, children:[
    Navigate{ key:"history", nav:History, children:[
      List{ key:"list", iter:HistoryList, children:[
        Leaf{ key:"timestamp", leaf:Timestamp, input:top },
        Leaf{ key:"eventId",   leaf:EventId,   input:top },
      ]}
    ]}
  ]}
]}
```

### Async / blocking
The existing resolvers offload to rayon via `blocking_compute`. The POC executor
is synchronous CPU work over an already-loaded graph. The one genuinely async
step — loading the graph in `graph(path)` — happens up front (await
`get_graph_with_read_permission`), after which the whole plan executes
synchronously, ideally inside one `blocking_compute` so we don't block the tokio
runtime. See [Q3](#q3-asyncblocking-execution).

---

## 10. Module layout (proposed)

```
src/interpreter/
  mod.rs        // Interpreter entry: run(query, Data, ctx) -> bytes
  value.rs      // Value enum
  plan.rs       // Op, Nav, IterKind, LeafKind, Args, Key, StackSlot
  planner.rs    // validated AST -> Plan
  validate.rs   // schema.graphql -> dynamic schema; document validation
  exec.rs       // exec loop + stack
  sink.rs       // streaming JSON Sink
```

---

## 11. Testing strategy

The engine is built to be **differentially tested** against the current one:

1. **Reuse the Python integration tests.** `test_graph_windows_and_layers_query`
   and friends in `test_graphql.py` already pin exact JSON. Running the same
   queries through the interpreter and asserting equality to the existing
   endpoint output is the primary correctness gate.
2. **Rust-level differential harness.** For a fixed graph, run a list of queries
   through both `Schema::execute` and `Interpreter::run` and assert the
   serialized JSON is identical (after canonical formatting).
3. **POC acceptance:** the `graph → node → history → list → {timestamp,eventId}`
   query returns byte-identical output to the current endpoint on the same graph.

---

## 12. Allocation budget

| Phase    | Allocations                                                        |
|----------|--------------------------------------------------------------------|
| Parse    | AST (owned by async-graphql-parser) — unavoidable, freed after plan|
| Validate | none beyond parser's                                               |
| Plan     | one `Plan` tree (`Box<[Op]>` per selection set) — built once       |
| Execute  | the reused `Vec<Value>` stack + the ~4Kb Sink buffer; **nothing per result** beyond one `Vec<u8>` per shipped chunk (ownership moves to poem) |

The headline property — “no allocation other than the stack of the plan” — holds
for the execute phase. Iterators (`history.iter()`, `neighbours`) are consumed
lazily and never `collect()`ed, and the response is streamed in ~4Kb chunks
rather than buffered whole. The only recurring allocation is one `Vec<u8>` per
chunk handed to the channel (see [Q-B](#q-b-buffer-recycling)).

---

## Open Questions

**All resolved** — see [Decisions locked](#decisions-locked-2026-06-20). Q1–Q7
from the initial draft and the streaming follow-ups Q-A (bounded tokio mpsc,
cap 8), Q-B (fresh `Vec` per chunk), Q-C (abort stream on impossible mid-stream
error), and Q-D (harness may collect for comparison) are all settled. No blocking
questions remain; the next step is implementation.

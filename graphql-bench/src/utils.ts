import http, { RefinedResponse, ResponseType } from "k6/http";
import {
  generateMutationOp,
  generateQueryOp,
  MutRootGenqlSelection,
  QueryRootGenqlSelection,
} from "./__generated";
import { GraphqlOperation } from "./__generated/runtime";
import { Counter, Rate, Trend } from "k6/metrics";
import { check, fail } from "k6";
import exec from "k6/execution";

const URL = __ENV.RAPHTORY_URL ?? "http://localhost:1736";

// ---------- read/write stats ----------------
// Every request is labelled with a kind (read/write) and an op name, which feeds three places:
//
//   * `read_*` / `write_*` metrics, so the summary reports duration, throughput and error rate
//     for reads and writes separately instead of lumping them into http_req_duration.
//   * an `op_<name>` trend per op, to tell apart the ops that share a kind (the composed read
//     query is nothing like the small `graph_size` read it is built on).
//   * `{kind, op}` tags on the HTTP request, so the CSV output and the web dashboard can be
//     sliced the same way after the run.

export type Kind = "read" | "write";
export type OpLabel = { op: string; kind: Kind };

type KindMetrics = { duration: Trend; reqs: Counter; errors: Rate };

const kindMetrics: Record<Kind, KindMetrics> = {
  read: {
    duration: new Trend("read_duration", true),
    reqs: new Counter("read_reqs"),
    errors: new Rate("read_errors"),
  },
  write: {
    duration: new Trend("write_duration", true),
    reqs: new Counter("write_reqs"),
    errors: new Rate("write_errors"),
  },
};

// k6 only accepts metrics created in the init context, so every op has to be declared while the
// module tree is loading: call `defineOp` at the top level of the test script, never inside a
// query function.
const opDurations: Record<string, Trend> = {};

export function defineOp(op: string, kind: Kind): OpLabel {
  if (!(op in opDurations)) {
    opDurations[op] = new Trend(`op_${op}`, true);
  }
  return { op, kind };
}

// Requests made without an explicit label still get the read/write split, just at the coarsest
// granularity GraphQL gives us for free.
const DEFAULT_QUERY_OP = defineOp("query", "read");
const DEFAULT_MUTATION_OP = defineOp("mutation", "write");

function recordStats({ label, response }: Exchange, ok: boolean) {
  const tags = { kind: label.kind, op: label.op };
  const { duration, reqs, errors } = kindMetrics[label.kind];
  duration.add(response.timings.duration, tags);
  reqs.add(1, tags);
  errors.add(!ok, tags);
  opDurations[label.op]?.add(response.timings.duration, tags);
}
// -----------------------------------------------------

// ---------- request/response sampling ----------------
// Every sample is a single JSON object holding both the request and its response, printed
// through console.log as one line. Run k6 with `--log-format=raw --console-output=<file>`
// and the file gets appended with clean JSONL:
//
//   SAMPLE_RATE=0.01 k6 run --log-format=raw --console-output=samples.jsonl \
//     dist/stress-test.js
//
// or just `make stress-test-sampled`. Since request and response live on the same line
// there is nothing to correlate afterwards; the `id` field (`vu<n>-iter<n>-req<n>`) is
// there so a specific pair can be referred to.
//
// A pair is recorded when it fails, when it is slower than SLOW_MS, or with probability
// SAMPLE_RATE; `reason` says which of the three it was. Slow capture is what turns a `max=3.5s`
// in the summary into the actual query text that took 3.5s, so it is on by default (and reads
// nothing back: sort the file by `.timings.duration` afterwards, e.g. `make slowest`).

// chance of recording any given request/response pair, 0 disables sampling
const SAMPLE_RATE = Number(__ENV.SAMPLE_RATE ?? "0");
// record every failing pair regardless of SAMPLE_RATE
const SAMPLE_ERRORS = (__ENV.SAMPLE_ERRORS ?? "true") !== "false";
// record every pair at least this slow (in ms) regardless of SAMPLE_RATE, 0 disables
const SLOW_MS = Number(__ENV.SLOW_MS ?? "1000");
// responses longer than this are stored as a truncated string instead of parsed JSON
const SAMPLE_MAX_CHARS = Number(__ENV.SAMPLE_MAX_CHARS ?? "20000");

// module state is per-VU in k6, so this only ever needs to be unique within a VU
let requestSeq = 0;

type Exchange = {
  id: string;
  kind: "query" | "mutation";
  label: OpLabel;
  operation: GraphqlOperation;
  response: RefinedResponse<ResponseType | undefined>;
};

function post(
  kind: "query" | "mutation",
  operation: GraphqlOperation,
  label: OpLabel,
): Exchange {
  requestSeq += 1;
  const id = `vu${exec.vu.idInTest}-iter${exec.vu.iterationInScenario}-req${requestSeq}`;
  const payload = JSON.stringify(operation);
  const response = http.post(URL, payload, {
    ...params,
    tags: { kind: label.kind, op: label.op },
  });
  return { id, kind, label, operation, response };
}

function parseBody(body: string) {
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

type SampleReason = "error" | "slow" | "sampled";

function sampleReason(
  response: RefinedResponse<ResponseType | undefined>,
  ok: boolean,
): SampleReason | null {
  if (!ok && SAMPLE_ERRORS) return "error";
  if (SLOW_MS > 0 && response.timings.duration >= SLOW_MS) return "slow";
  if (Math.random() < SAMPLE_RATE) return "sampled";
  return null;
}

function recordSample(
  { id, kind, label, operation, response }: Exchange,
  ok: boolean,
) {
  const reason = sampleReason(response, ok);
  if (reason === null) return;

  const body = typeof response.body === "string" ? response.body : null;
  const truncated = body !== null && body.length > SAMPLE_MAX_CHARS;
  const { duration, blocked, connecting, sending, waiting, receiving } =
    response.timings;

  console.log(
    JSON.stringify({
      id,
      ok,
      reason,
      kind,
      op: label.op,
      opKind: label.kind,
      time: new Date().toISOString(),
      vu: exec.vu.idInTest,
      iteration: exec.vu.iterationInScenario,
      status: response.status,
      durationMs: duration,
      // waiting is the server's own time, the rest tells apart a slow server from a queued client
      timings: { duration, blocked, connecting, sending, waiting, receiving },
      request: { query: operation.query, variables: operation.variables },
      response:
        body === null
          ? null
          : truncated
            ? body.slice(0, SAMPLE_MAX_CHARS)
            : parseBody(body),
      ...(truncated ? { truncated: true, responseChars: body!.length } : {}),
    }),
  );
}
// -----------------------------------------------------

function responseOk(response: RefinedResponse<ResponseType | undefined>) {
  if (response.status !== 200 || typeof response.body !== "string") {
    return false;
  }
  const body = parseBody(response.body);
  // FIXME: improve query checking, I wish I could just rely on genql
  return (
    typeof body === "object" &&
    body !== null &&
    "data" in body &&
    body.data !== undefined &&
    body.data !== null
  );
}

// every request goes through here exactly once, so stats and samples stay in step
function finish(exchange: Exchange, ok: boolean) {
  recordStats(exchange, ok);
  recordSample(exchange, ok);
  return ok;
}

function checkResponse(exchange: Exchange, errorRate: Rate) {
  const ok = responseOk(exchange.response);
  check(exchange.response, {
    "response status is 200": (r) => r.status === 200,
    "response has data field defined": () => ok,
  });
  errorRate.add(!ok, { kind: exchange.label.kind, op: exchange.label.op });
  return finish(exchange, ok);
}

const params = {
  headers: { "Content-Type": "application/json", "Accept-Encoding": "gzip" },
};

function fetch(query: QueryRootGenqlSelection, op: OpLabel) {
  return post("query", generateQueryOp(query), op);
}

export function mutate(
  query: MutRootGenqlSelection,
  op: OpLabel = DEFAULT_MUTATION_OP,
) {
  const exchange = post("mutation", generateMutationOp(query), op);
  finish(exchange, responseOk(exchange.response));
  return exchange.response;
}

export function fetchAndParse(
  query: QueryRootGenqlSelection,
  op: OpLabel = DEFAULT_QUERY_OP,
) {
  const exchange = fetch(query, op);
  const { response } = exchange;
  if (typeof response.body !== "string") {
    finish(exchange, false);
    fail(JSON.stringify(response));
  }
  finish(exchange, responseOk(response));
  return JSON.parse(response.body);
}

export function fetchAndCheck(
  errorRate: Rate,
  query: QueryRootGenqlSelection,
  op: OpLabel = DEFAULT_QUERY_OP,
) {
  checkResponse(fetch(query, op), errorRate);
}

export function mutateAndCheck(
  errorRate: Rate,
  query: MutRootGenqlSelection,
  op: OpLabel = DEFAULT_MUTATION_OP,
) {
  checkResponse(post("mutation", generateMutationOp(query), op), errorRate);
}

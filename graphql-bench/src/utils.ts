import http, { RefinedResponse, ResponseType } from "k6/http";
import {
  generateMutationOp,
  generateQueryOp,
  MutRootGenqlSelection,
  QueryRootGenqlSelection,
} from "./__generated";
import { GraphqlOperation } from "./__generated/runtime";
import { Rate } from "k6/metrics";
import { check, fail } from "k6";
import exec from "k6/execution";

const URL = __ENV.RAPHTORY_URL ?? "http://localhost:1736";

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

// chance of recording any given request/response pair, 0 disables sampling
const SAMPLE_RATE = Number(__ENV.SAMPLE_RATE ?? "0");
// record every failing pair regardless of SAMPLE_RATE
const SAMPLE_ERRORS = (__ENV.SAMPLE_ERRORS ?? "true") !== "false";
// responses longer than this are stored as a truncated string instead of parsed JSON
const SAMPLE_MAX_CHARS = Number(__ENV.SAMPLE_MAX_CHARS ?? "20000");

// module state is per-VU in k6, so this only ever needs to be unique within a VU
let requestSeq = 0;

type Exchange = {
  id: string;
  kind: "query" | "mutation";
  operation: GraphqlOperation;
  response: RefinedResponse<ResponseType | undefined>;
};

function post(kind: "query" | "mutation", operation: GraphqlOperation): Exchange {
  requestSeq += 1;
  const id = `vu${exec.vu.idInTest}-iter${exec.vu.iterationInScenario}-req${requestSeq}`;
  const payload = JSON.stringify(operation);
  const response = http.post(URL, payload, params);
  return { id, kind, operation, response };
}

function parseBody(body: string) {
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

function recordSample({ id, kind, operation, response }: Exchange, ok: boolean) {
  const forced = !ok && SAMPLE_ERRORS;
  if (!forced && Math.random() >= SAMPLE_RATE) return;

  const body = typeof response.body === "string" ? response.body : null;
  const truncated = body !== null && body.length > SAMPLE_MAX_CHARS;

  console.log(
    JSON.stringify({
      id,
      ok,
      kind,
      time: new Date().toISOString(),
      vu: exec.vu.idInTest,
      iteration: exec.vu.iterationInScenario,
      status: response.status,
      durationMs: response.timings.duration,
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

function checkResponse(exchange: Exchange, errorRate: Rate) {
  const ok = responseOk(exchange.response);
  check(exchange.response, {
    "response status is 200": (r) => r.status === 200,
    "response has data field defined": () => ok,
  });
  errorRate.add(!ok);
  recordSample(exchange, ok);
  return ok;
}

const params = {
  headers: { "Content-Type": "application/json", "Accept-Encoding": "gzip" },
};

function fetch(query: QueryRootGenqlSelection) {
  return post("query", generateQueryOp(query));
}

export function mutate(query: MutRootGenqlSelection) {
  const exchange = post("mutation", generateMutationOp(query));
  recordSample(exchange, responseOk(exchange.response));
  return exchange.response;
}

export function fetchAndParse(query: QueryRootGenqlSelection) {
  const exchange = fetch(query);
  const { response } = exchange;
  if (typeof response.body !== "string") {
    recordSample(exchange, false);
    fail(JSON.stringify(response));
  }
  recordSample(exchange, responseOk(response));
  return JSON.parse(response.body);
}

export function fetchAndCheck(errorRate: Rate, query: QueryRootGenqlSelection) {
  checkResponse(fetch(query), errorRate);
}

export function mutateAndCheck(errorRate: Rate, query: MutRootGenqlSelection) {
  checkResponse(post("mutation", generateMutationOp(query)), errorRate);
}

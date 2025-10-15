import http, { RefinedResponse } from "k6/http";
import { generateMutationOp, generateQueryOp, MutRootGenqlSelection, QueryRootGenqlSelection } from "./__generated";
import { Rate } from "k6/metrics";
import { check, fail } from "k6";

const URL = __ENV.RAPHTORY_URL ?? 'http://localhost:1736';

function checkResponse<RT extends "binary" | "none" | "text"| undefined>(response: RefinedResponse<RT>, errorRate: Rate) {
  const result = check(response, {
      'response status is 200': (r) => r.status === 200,
      'response has data field defined': (r) => {
          if (typeof r.body === 'string') {
              const body = JSON.parse(r.body);
              const result = 'data' in body &&
                body.data !== undefined &&
                body.data !== null; // FIXME: improve query checking, I wish I could just rely on genql

              if (result === false) {
                // console.log(">>> error:", JSON.stringify(body, null, 2));
                // console.log(">>> request:", JSON.stringify(response.request.body, null, 2))
              }

              return result;
          } else {
              return false;
          }
      },
  });
  errorRate.add(!result);
}

const params = {
    headers: { 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip' },
};
function fetch(query: QueryRootGenqlSelection) {
    const { query: compiledQuery, variables } = generateQueryOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return http.post(URL, payload, params);
}

export function mutate(query: MutRootGenqlSelection) {
    const { query: compiledQuery, variables } = generateMutationOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return http.post(URL, payload, params);
}

export function fetchAndParse(query: QueryRootGenqlSelection) {
    const response = fetch(query);
    if (typeof response.body !== 'string') {
        fail(JSON.stringify(response));
    }
    return JSON.parse(response.body);
}

export function fetchAndCheck(errorRate: Rate, query: QueryRootGenqlSelection, ) {
    checkResponse(fetch(query), errorRate);
}

export function mutateAndCheck(errorRate: Rate, query: MutRootGenqlSelection, ) {
    checkResponse(mutate(query), errorRate);
}

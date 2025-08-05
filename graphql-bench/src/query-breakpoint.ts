import { check, fail, sleep } from 'k6';
import http from 'k6/http';
import { Rate } from 'k6/metrics';

import { generateQueryOp, QueryRootGenqlSelection } from './__generated';
import { queries } from './queries';

export const errorRate = new Rate('errors');

const loadTestOptions = {
    stages: [
        { duration: '30s', target: 200 },
        { duration: '30s', target: 400 },
        { duration: '30s', target: 800 },
        { duration: '1m', target: 800 },
        { duration: '30s', target: 1600 },
        { duration: '1m', target: 1600 },
        { duration: '30s', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],
        errors: ['rate<0.05'],
        http_req_failed: ['rate<0.01'],
    },
};

const abortOnFail = false;
const delayAbortEval = '10s';

// const breakpointOptions = {
//     executor: 'ramping-arrival-rate',
//     stages: [{ duration: '10m', target: 5000 }],
//     thresholds: {
//         http_req_duration: [
//             { threshold: 'p(95)<500', abortOnFail, delayAbortEval },
//         ],
//         errors: [{ threshold: 'rate<0.05', abortOnFail, delayAbortEval }],
//         http_req_failed: [
//             { threshold: 'rate<0.01', abortOnFail, delayAbortEval },
//         ],
//     },
// };

const singleVuOptions = {
    scenarios: {
        ramping_iterations: {
            executor: 'ramping-arrival-rate',
            startRate: 0,
            timeUnit: '1s',
            preAllocatedVUs: 5,
            maxVUs: 3000,
            stages: [
                // { duration: '30s', target: 20 },
                // { duration: '30s', target: 50 },
                // { duration: '30s', target: 100 },
                // { duration: '30s', target: 200 },
                // { duration: '30s', target: 400 },
                // { duration: '30s', target: 800 },
                // /////////////////////////
                { duration: '5m', target: 20_000 },
            ],
        },
    },
    thresholds: {
        http_req_duration: [
            { threshold: 'p(95)<500', abortOnFail, delayAbortEval },
        ],
        errors: [{ threshold: 'rate<0.05', abortOnFail, delayAbortEval }],
        http_req_failed: [
            { threshold: 'rate<0.01', abortOnFail, delayAbortEval },
        ],
    },
};

// TODO: maybe use some of the seetings below instead, setting timeUnit: '1s' makes the test work by iterations/s instead of vus

// const genericScenarios = Object.fromEntries(
//     queries.map((query, index) => [
//         query.name,
//         {
//             exec: query.name,
//             startTime: `${index * mins}m`,
//             executor: 'ramping-arrival-rate',
//             stages: [{ duration: `${mins}m`, target: 50_000 }],
//             timeUnit: '1s',
//             preAllocatedVUs: 50,
//             maxVus: 3_000,
//             thresholds: {
//                 http_req_duration: [
//                     { threshold: 'p(95)<500', abortOnFail, delayAbortEval },
//                 ],
//                 errors: [
//                     { threshold: 'rate<0.05', abortOnFail, delayAbortEval },
//                 ],
//                 http_req_failed: [
//                     { threshold: 'rate<0.01', abortOnFail, delayAbortEval },
//                 ],
//             },
//         },
//     ]),
// );

export const options = singleVuOptions;

const selectedQuery = __ENV.QUERY;
const query = queries[selectedQuery];

const url = __ENV.RAPHTORY_URL ?? 'http://localhost:1736';
const params = { headers: { 'Content-Type': 'application/json' } };
function fetch(query: QueryRootGenqlSelection) {
    const { query: compiledQuery, variables } = generateQueryOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return http.post(url, payload, params);
}

export async function setup() {
    // const response = fetch({
    //     namespaces: { list: { graphs: { list: { path: true } } } },
    // });
    // if (typeof response.body !== 'string') {
    //     fail('Initial graph list query failed');
    // }
    // const data = JSON.parse(response.body);
    // const paths = data.data.namespaces.list.flatMap((ns: any) =>
    //     ns.graphs.list.map((graph: any) => graph.path),
    // );
    // return paths;

    const { query: compiledQuery, variables } = generateQueryOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return payload;
}

export default function (payload: any) {
    http.post(url, payload, params);
    // const response = fetch(query(paths));
    // const result = check(response, {
    //     'variable query status is 200': (r) => r.status === 200,
    //     // 'variable query has user data': (r) => {
    //     //     if (typeof r.body === 'string') {
    //     //         const body = JSON.parse(r.body as string);
    //     //         return (
    //     //             'data' in body &&
    //     //             body.data !== undefined &&
    //     //             body.data !== null // FIXME: improve query checking, I wish I could just rely on genql
    //     //         );
    //     //     } else {
    //     //         return false;
    //     //     }
    //     // },
    // });
    // errorRate.add(!result);
}

export function handleSummary(data: any) {
    const vus = data.metrics.vus.values.max;
    const iters = data.metrics.iterations?.values?.rate ?? 0;
    return {
        [`breakpoints/${selectedQuery}.csv`]: `${selectedQuery},${vus},${iters}\n`, //the default data object
    };

    // return {
    //   'summary.json': JSON.stringify(data), //the default data object
    // };
}

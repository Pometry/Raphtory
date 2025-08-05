import { execSync, spawn } from 'child_process';
import { queries } from './queries';
import fs from 'fs';
import { z } from 'zod';

execSync('rm -f breakpoints.csv && rm -fr breakpoints && mkdir -p breakpoints');

const queryNames = Object.keys(queries);
for (const queryName of queryNames) {
    try {
        execSync(
            // `QUERY=${queryName} K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=report.html k6 run --out json=output.json dist/query-breakpoint.js;`,
            `QUERY=${queryName} K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=/tmp/bench/report.html k6 run dist/query-breakpoint.js;`,
            {
                stdio: 'inherit',
            },
        );
    } catch {}
}

execSync(
    'echo scenario,vus,iters > breakpoints.csv && cat breakpoints/* >> breakpoints.csv',
);

// const PointSchema = z.object({
//     metric: z.enum(['http_req_duration']),
//     type: z.literal('Point'),
//     data: z.object({
//         time: z.coerce.date(),
//         tags: z.object({ scenario: z.string(), status: z.coerce.number() }),
//     }),
// });

// const fileContent = fs.readFileSync('yourfile.jsonl', 'utf-8');
// const lines = fileContent.split('\n').filter((line) => line.trim() !== '');
// const points = lines.flatMap(
//     (line) => PointSchema.safeParse(JSON.parse(line)).data ?? [],
// );

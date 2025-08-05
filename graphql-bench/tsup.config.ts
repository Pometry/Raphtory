import { defineConfig } from 'tsup';

export default defineConfig({
    entry: [
        'src/index.ts',
        'src/test.ts',
        'src/breakpoint.ts',
        'src/query-breakpoint.ts',
        'src/setup-server.ts',
    ],
    format: ['esm'], // or ['cjs', 'esm']
    external: ['k6', 'k6/http', 'k6/metrics'],
});

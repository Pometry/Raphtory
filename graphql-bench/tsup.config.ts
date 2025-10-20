import { defineConfig } from 'tsup';

export default defineConfig({
    entry: ['src/bench.ts', 'src/stress-test.ts'],
    format: ['esm'], // or ['cjs', 'esm']
    external: ['k6', 'k6/http', 'k6/metrics'],
});

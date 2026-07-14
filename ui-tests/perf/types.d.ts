// ui-tests/perf/types.d.ts

export {};

declare global {
    interface Window {
        __PERF__?: {
            dump: () => {
                operation: 'drag' | 'bulk-add' | 'zoom';
                startedAt: number;
                endedAt: number;
                durationMs: number;
                metadata?: {
                    nodeCount?: number;
                    fps?: number;
                    frameCount?: number;
                };
            }[];
            clear: () => void;
        };
    }
}

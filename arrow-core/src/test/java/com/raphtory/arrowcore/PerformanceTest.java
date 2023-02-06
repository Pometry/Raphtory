package com.raphtory.arrowcore;

import java.util.Date;

public class PerformanceTest {
    public static final int N_ITEMS = 10_000_000;

    public static void main(String[] args) throws Exception {
        DataSetBuilder b = new DataSetBuilder(1);

        long then = System.currentTimeMillis();
        System.out.println(new Date() + ": PERF Test starting...");

        //b.addVertex(0, 0);

        for (int i=0; i<N_ITEMS; ++i) {
            b.addVertex(i, i);

            b.addEdge(i, i, i); // Edge back to self

            if (i>0) {
                b.addEdge(i - 1, i, i); // Edge from prev to this
            }

            if (i>8) {
                b.addEdge(i-8, i, i); // Bi-directional edge from this-8 to this
                b.addEdge(i, i-8, i);
            }
        }

        long now = System.currentTimeMillis();

        System.out.println(new Date() + ": PERF Test ended...");
        System.out.println(new Date() + " Processed " + N_ITEMS + " vertices and " + (N_ITEMS * 4) + " edges in " + (now - then) + "ms");
    }
}
package com.raphtory.arrowcore;

import org.junit.Test;

public class BaseTests {
    private static final long[][] _data = new long[][]{
            {1,2,1},
            {1,3,2},
            {1,4,3},
            {3,1,4},
            {3,4,5},
            {3,5,6},
            {4,5,7},
            {5,6,8},
            {5,8,9},
            {7,5,10},
            {8,5,11},
            {1,9,12},
            {9,1,13},
            {6,3,14},
            {4,8,15},
            {8,3,16},
            {5,10,17},
            {10,5,18},
            {10,8,19},
            {1,11,20},
            {11,1,21},
            {9,11,22},
            {11,9,23},

            {3,3,24} // self-edge
    };


    @Test
    public void testSingleRAPCreation() {
        DataSetBuilder b = new DataSetBuilder(1);
        b.setData(_data, true);
    }


    @Test
    public void testMultiRapCreation() {
        for (int i=2; i<8; ++i) {
            DataSetBuilder b = new DataSetBuilder(i);
            b.setData(_data, true);
        }
    }


    //@Test
    public void testVerticesAcrossMultipleFiles() {
        DataSetBuilder b = new DataSetBuilder(1);
        b.setData(_data, true);
    }

    /*
    @Test
    public void testEdgesAcrossMultipleFiles() {}

    @Test
    public void testProperties() {}

    @Test
    public void testFields() {}
*/
    // History, VertexEdgeIndex,
}


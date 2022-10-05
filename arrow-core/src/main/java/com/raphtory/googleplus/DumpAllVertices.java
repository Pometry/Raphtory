/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;

/**
 * Sample program that outputs vertex details for all vertices.
 * Single-threaded!
 */
public class DumpAllVertices {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();



    public static void main(String[] args) throws Exception {
        String src = args[0];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new GPSchema();
        cfg._arrowDir = src;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._vertexPartitionSize = 4096;
        cfg._edgePartitionSize = 512 * 1024;
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        new DumpAllVertices(rap).dump();

        System.exit(0);
    }


    public DumpAllVertices(RaphtoryArrowPartition rap) {
        _rap = rap;

        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        _aepm.loadFiles();
        _avpm.loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": GP: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");
    }


    public void dump() throws Exception {
        long then = System.currentTimeMillis();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in), 16384);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        int USERID_FIELD = _rap.getVertexFieldId("userid");

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
            out.append(": ");
            out.append(Long.toString(iter.getGlobalVertexId()));
            out.append(" -> ");
            out.append(iter.getField(USERID_FIELD).getString());

            out.append(", nInEdges=" + iter.getNIncomingEdges() + ", nOutEdges=" + iter.getNOutgoingEdges());

            EdgeIterator edges = iter.getIncomingEdges();

            int edgeNum = 0;
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    " + edgeNum++ + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    " + edgeNum++ + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());
            }

            out.append("\n");
        }
        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": GP: VERTEX PRINTING TOOK: " + (now - then) + "ms");
    }
}
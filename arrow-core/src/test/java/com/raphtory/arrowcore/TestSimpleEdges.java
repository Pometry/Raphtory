/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */
package com.raphtory.arrowcore;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Simple program to confirm that vertices and edges are
 * retrieved correctly.
 */
public class TestSimpleEdges {
    private static final StringBuilder _tmpSB = new StringBuilder();

    private static RaphtoryArrowPartition _rap;
    private static VertexPartitionManager _avpm;
    private static EdgePartitionManager _aepm;
    private static int VFIELD_NAME;

    private static final HashMap<String, Vertex> _vertices = new HashMap<>();
    private static String[] _names = new String[]{
            "Bob",
            "Carol",
            "Ted",
            "Alice"
    };



    @BeforeClass
    public static void setup() throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new SimpleTestSchema();
        cfg._arrowDir = "/tmp";
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 1;
        cfg._localEntityIdMapSize = 8;
        cfg._syncIDMap = false;
        cfg._edgePartitionSize = 512;
        cfg._vertexPartitionSize = 32;

        _rap = new RaphtoryArrowPartition(cfg);
        _aepm = _rap.getEdgeMgr();
        _avpm = _rap.getVertexMgr();
        VFIELD_NAME = _rap.getVertexFieldId("name");

        setData();
    }


    private static void setData() throws Exception {
        long time = System.currentTimeMillis();

        for (int i=0; i<_names.length; ++i) {
            Vertex v = _rap.getVertex();
            v.reset(i, i, true, time);
            v.getField(VFIELD_NAME).set(_names[i]);
            _avpm.addVertex(v);
            _avpm.addHistory(i, time, true, true, -1L, false);
        }

        for (int i=0; i<_names.length; ++i) {
            Vertex v = _avpm.getVertex(i);
            assertNotNull(v);
            assertEquals(_names[i], v.getField(VFIELD_NAME).getString().toString());
            _vertices.put(_names[i], v);
        }

        Vertex bob = _vertices.get("Bob");
        assertNotNull(bob);
        Vertex alice = _vertices.get("Alice");
        assertNotNull(alice);

        Edge e = _rap.getEdge();
        e.reset(0, 0, true, time);
        e.resetEdgeData(bob.getLocalId(), alice.getLocalId(), false, false);
        _aepm.addEdge(e, -1L, -1L);
        _aepm.addHistory(e.getLocalId(), time, true, false);

        VertexPartition p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(bob.getLocalId()));
        _aepm.setOutgoingEdgePtr(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
        p.addHistory(bob.getLocalId(), System.currentTimeMillis(), true, false, e.getLocalId(), true);

        p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(alice.getLocalId()));
        _aepm.setIncomingEdgePtr(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
        p.addHistory(alice.getLocalId(), System.currentTimeMillis(), true, false, e.getLocalId(), false);

        VertexIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            System.out.println("VERTEX: " + vi.next() + " -> " + vi.getField(VFIELD_NAME).getString());
        }
    }


    @Test
    public void allVerticesIterator() throws Exception {
        HashSet<String> foundVertices = new HashSet<>();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            long id = iter.next();
            String name = getVertexName(iter).toString();

            assertFalse(foundVertices.contains(name));

            foundVertices.add(name);

            out.append(Long.toString(id));
            out.append(": ");
            out.append(name);
            out.append(", nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

            EdgeIterator edges = iter.getIncomingEdges();

            int edgeNum = 0;
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    I " + edgeNum + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());

                ++edgeNum;
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    O " + edgeNum + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());

                ++edgeNum;
            }

            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": allVerticesIterator finished");
    }


    @Test
    public void allEdgesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Edges:");
        EdgeIterator ei = _rap.getNewAllEdgesIterator();
        ei.reset(0); // fix me
        while (ei.hasNext()) {
            long edgeId = ei.next();

            out.append("\n    " + edgeId + " -> ");
            out.append("f=" + ei.getSrcVertexId());
            out.append(", t=" + ei.getDstVertexId());
            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": All edges finished");
    }


    @Test
    public void allWindowedVerticesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Windowed Vertices:");
        VertexIterator.WindowedVertexIterator iter = _rap.getNewWindowedVertexIterator(0L, Long.MAX_VALUE);
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
            out.append(": ");
            out.append(getVertexName(iter));
            out.append(", nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

            EdgeIterator edges = iter.getIncomingEdges();

            int edgeNum = 0;
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    I " + edgeNum + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());

                ++edgeNum;
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    O " + edgeNum + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());

                ++edgeNum;
            }

            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": allWindowedVerticesIterator finished");
    }


    @Test
    public void allWindowedEdgesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Windowed Edges:");
        EdgeIterator ei = _rap.getNewWindowedEdgeIterator(0L, Long.MAX_VALUE);
        while (ei.hasNext()) {
            long edgeId = ei.next();

            out.append("\n    " + edgeId + " -> ");
            out.append("f=" + ei.getSrcVertexId());
            out.append(", t=" + ei.getDstVertexId());
            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": All Windowed edges finished");
    }




    private StringBuilder getVertexName(Vertex v) {
        StringBuilder name = _tmpSB;

        name.setLength(0);

        StringBuilder sb = v.getField(VFIELD_NAME).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
        }

        return name;
    }


    private StringBuilder getVertexName(VertexIterator iter) {
        StringBuilder name = _tmpSB;

        name.setLength(0);

        StringBuilder sb = iter.getField(VFIELD_NAME).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
        }
        return name;
    }
}
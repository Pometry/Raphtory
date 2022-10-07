/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.test;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.PropertySchema;
import com.raphtory.arrowcore.model.Vertex;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * Simple program to confirm that vertices and edges are
 * retrieved correctly.
 */
public class FabianTest {
    public static class TestSchema implements PropertySchema {
        private static final ArrayList<NonversionedField> _nonVersionedVertexProperties;
        private static final ArrayList<VersionedProperty>    _versionedVertexProperties;

        private static final ArrayList<NonversionedField> _nonVersionedEdgeProperties;
        private static final ArrayList<VersionedProperty>    _versionedEdgeProperties;

        static {
            _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                    new NonversionedField("transaction_hash", StringBuilder.class),
                    new NonversionedField("address_chain",    StringBuilder.class),
                    new NonversionedField("is_halt",          boolean.class)
            ));

            _nonVersionedEdgeProperties = new ArrayList<>(Arrays.asList(
                    new NonversionedField("amount",      long.class),
                    new NonversionedField("is_received", boolean.class)
            ));

            //_versionedVertexProperties = null;
            //_versionedEdgeProperties   = null;

            /* */
            _versionedVertexProperties = new ArrayList<>(Arrays.asList(
                    // Dummy field used for testing
                    new VersionedProperty("magic_vertex", long.class)
            ));

            _versionedEdgeProperties = new ArrayList<>(Arrays.asList(
                    // Dummy field used for testing
                    new VersionedProperty("magic_edge", long.class)
            ));
            /* */
        }


        @Override
        public ArrayList<NonversionedField> nonversionedVertexProperties() {
            return _nonVersionedVertexProperties;
        }


        @Override
        public ArrayList<VersionedProperty> versionedVertexProperties() {
            return _versionedVertexProperties;
        }


        @Override
        public ArrayList<NonversionedField> nonversionedEdgeProperties() {
            return _nonVersionedEdgeProperties;
        }


        @Override
        public ArrayList<VersionedProperty> versionedEdgeProperties() {
            return _versionedEdgeProperties;
        }
    }


    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;



    public static void main(String[] args) throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new TestSchema();
        cfg._arrowDir = "/tmp";
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 1;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        FabianTest it = new FabianTest(rap);
        it.setData();
        it.allVerticesIterator();
        it.allEdgesIterator();
        it.allWindowedVerticesIterator();
        it.allWindowedEdgesIterator();

        it.fabianTest(0, 5);
        it.fabianTest(0, 10);
        it.fabianTest(0, 16);

        System.exit(0);
    }


    public FabianTest(RaphtoryArrowPartition rap) {
        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();
    }


    public void setData() throws Exception {
        Vertex three = createVertex(_rap.getVertexMgr().getNextFreeVertexId(), 3, 1);
        Vertex five = createVertex(_rap.getVertexMgr().getNextFreeVertexId(), 5, 1);
        Vertex six = createVertex(_rap.getVertexMgr().getNextFreeVertexId(), 6, 1);

        addRemoteSourceEdge(5, six, 8);

        addVerticesToEdge(six, three, 14);
    }


    private void addRemoteSourceEdge(long globalSrcId, Vertex dst, long time) {
        Edge e = _rap.getEdge();
        e.init(_rap.getEdgeMgr().getNextFreeEdgeId(), true, time);
        e.resetEdgeData(globalSrcId, dst.getLocalId(), -1L, -1L, true, false);
        _rap.getEdgeMgr().addEdge(e, -1L, -1L);
        _rap.getEdgeMgr().addHistory(e.getLocalId(), time, true, true);

        //VertexPartition p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(src.getLocalId()));
        //_rap.getEdgeMgr().setOutgoingEdgePtr(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex()));
        //p.addHistory(src.getLocalId(), time, true, false, e.getLocalId(), true);

        VertexPartition  p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(dst.getLocalId()));
        _rap.getEdgeMgr().setIncomingEdgePtr(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId()));
        p.addHistory(dst.getLocalId(), time, true, false, e.getLocalId(), false);
    }


    private void addVerticesToEdge(Vertex src, Vertex dst, long time) {
        Edge e = _rap.getEdge();
        e.init(_rap.getEdgeMgr().getNextFreeEdgeId(), true, time);
        e.resetEdgeData(src.getLocalId(), dst.getLocalId(), -1L, -1L, false, false);
        _rap.getEdgeMgr().addEdge(e, -1L, -1L);
        _rap.getEdgeMgr().addHistory(e.getLocalId(), time, true, true);

        VertexPartition p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(src.getLocalId()));
        _rap.getEdgeMgr().setOutgoingEdgePtr(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex()));
        p.addHistory(src.getLocalId(), time, true, false, e.getLocalId(), true);

        p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(dst.getLocalId()));
        _rap.getEdgeMgr().setIncomingEdgePtr(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId()));
        p.addHistory(dst.getLocalId(), time, true, false, e.getLocalId(), false);
    }


    private Vertex createVertex(long localId, long globalId, long time) {
        Vertex v = _rap.getVertex();
        v.reset(localId, globalId, true, time);
        v.getField(VFIELD_ADDRESS_CHAIN).set("" + globalId);
        _avpm.addVertex(v);
        return v;
    }


    public void fabianTest(long start, long end) {

        System.out.println("FABIAN TEST: start=" + start + ", end=" + end);

        long[] localVertexIds = new long[]{0, 1, 2};

        for (int i=0; i<localVertexIds.length; ++i) {
            VertexIterator.WindowedVertexIterator wvi = _rap.getNewWindowedVertexIterator(start, end);
            wvi.reset(localVertexIds[i]);
            long vertexId = wvi.next();
            Vertex v = wvi.getVertex();

            EdgeIterator ei = wvi.getIncomingEdges();
            while (ei.hasNext()) {
                ei.next();
                System.out.println(getVertexName(v) + " has INCEDGE: " + ei.getEdge().toString() + " time=" + ei.getCreationTime() + ", localsrc=" + ei.isSrcVertexLocal() + ", localdst=" + ei.isDstVertexLocal());
            }

            ei = wvi.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();
                System.out.println(getVertexName(v) + " has OUTEDGE: " + ei.getEdge().toString() + " time=" + ei.getCreationTime() + ", localsrc=" + ei.isSrcVertexLocal() + ", localdst=" + ei.isDstVertexLocal());
            }
        }
    }


    public void allVerticesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
            out.append(": ");
            out.append(getVertexName(iter.getVertex()));
            out.append("\n");
        }

        out.append("\n\n");

        iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
            out.append(": ");
            out.append(getVertexName(iter.getVertex()));
            out.append(" -> ");
            out.append("nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

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


    public void allEdgesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Edges:");
        EdgeIterator ei = _rap.getNewAllEdgesIterator();
        while (ei.hasNext()) {
            long edgeId = ei.next();

            out.append("\n    " + edgeId + " -> " + edgeId);
            out.append(", f=" + ei.getSrcVertexId());
            out.append(", t=" + ei.getDstVertexId());
            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": All edges finished");
    }


    public void allWindowedVerticesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Windowed Vertices:");
        VertexIterator.WindowedVertexIterator iter = _rap.getNewWindowedVertexIterator(0L, Long.MAX_VALUE);
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
            out.append(": ");
            out.append("nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

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


    public void allWindowedEdgesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        out.append("\n\nAll Windowed Edges:");
        EdgeIterator ei = _rap.getNewWindowedEdgeIterator(0L, Long.MAX_VALUE);
        while (ei.hasNext()) {
            long edgeId = ei.next();

            out.append("\n    " + edgeId + " -> " + edgeId);
            out.append(", f=" + ei.getSrcVertexId());
            out.append(", t=" + ei.getDstVertexId());
            out.append("\n");
        }

        out.flush();
        System.out.println(new Date() + ": All Windowed edges finished");
    }


    private StringBuilder getVertexName(Vertex v) {
        StringBuilder name = _tmpSB;

        name.setLength(0);

        StringBuilder sb = v.getField(VFIELD_ADDRESS_CHAIN).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
            return name;
        }

        name.append(v.getField(VFIELD_TX_HASH).getString());
        return name;
    }
}

/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.Vertex;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Date;

/**
 * Simple program to confirm that vertices and edges are
 * retrieved correctly.
 */
public class BitcoinTestEdges {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;



    public static void main(String[] args) throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new BitcoinSchema();
        cfg._arrowDir = "/tmp";
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 1;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        BitcoinTestEdges it = new BitcoinTestEdges(rap);
        it.setData();
        it.allVerticesIterator();
        it.allEdgesIterator();
        it.allWindowedVerticesIterator();
        it.allWindowedEdgesIterator();

        System.exit(0);
    }


    public BitcoinTestEdges(RaphtoryArrowPartition rap) {
        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();
    }


    public void setData() throws Exception {
        long time = System.currentTimeMillis();

        Vertex bob = _rap.getVertex();
        bob.reset(0, 0, true, time);
        bob.getField(VFIELD_TX_HASH).set("Bob");
        _avpm.addVertex(bob);

        Vertex alice = _rap.getVertex();
        alice.reset(1, 1, true, time);
        alice.getField(VFIELD_TX_HASH).set("Alice");
        _avpm.addVertex(alice);

        Edge e = _rap.getEdge();
        e.reset(0, 0, true, time);
        e.resetEdgeData(bob.getLocalId(), alice.getLocalId(), -1L, -1L, false, false);
        _rap.getEdgeMgr().addEdge(e, -1L, -1L);
        _rap.getEdgeMgr().addHistory(e.getLocalId(), time, true, true);

        VertexPartition p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(bob.getLocalId()));
        _rap.getEdgeMgr().setOutgoingEdgePtr(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex()));
        p.addHistory(bob.getLocalId(), System.currentTimeMillis(), true, false, e.getLocalId(), true);

        p = _rap.getVertexMgr().getPartition(_rap.getVertexMgr().getPartitionId(alice.getLocalId()));
        _rap.getEdgeMgr().setIncomingEdgePtr(e.getLocalId(), p.addIncomingEdgeToList(alice.getLocalId(), e.getLocalId()));
        p.addHistory(alice.getLocalId(), System.currentTimeMillis(), true, false, e.getLocalId(), false);


        VertexIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            System.out.println("VERTEX: " + vi.next() + " -> " + vi.getField(VFIELD_TX_HASH).getString());
        }
    }


    public void allVerticesIterator() throws Exception {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
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


    private StringBuilder getVertexName(VertexIterator iter) {
        StringBuilder name = _tmpSB;

        name.setLength(0);

        StringBuilder sb = iter.getField(VFIELD_ADDRESS_CHAIN).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
            return name;
        }

        name.append(iter.getField(VFIELD_TX_HASH).getString());
        return name;
    }
}
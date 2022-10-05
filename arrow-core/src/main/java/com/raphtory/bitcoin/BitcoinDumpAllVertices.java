/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Vertex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;

/**
 * Sample program that outputs vertex details for all vertices.
 * Single-threaded!
 */
public class BitcoinDumpAllVertices {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;
    private final int VPROPERTY_MAGIC;
    private final int EPROPERTY_MAGIC;



    public static void main(String[] args) throws Exception {
        String src = args[0];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new BitcoinSchema();
        cfg._arrowDir = src;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1 * 1024 * 1024;
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        new BitcoinDumpAllVertices(rap).dump();

        System.exit(0);
    }


    public BitcoinDumpAllVertices(RaphtoryArrowPartition rap) {
        _rap = rap;

        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");
        VPROPERTY_MAGIC = rap.getVertexPropertyId("magic_vertex");
        EPROPERTY_MAGIC = rap.getEdgePropertyId("magic_edge");

        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        _aepm.loadFiles();
        _avpm.loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");
    }


    public void dump() throws Exception {
        long then = System.currentTimeMillis();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in), 16384);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            long vertexId = iter.next();

            out.append(Long.toString(vertexId));
            out.append(": ");
            out.append(getVertexName(iter));
            out.append(", magic=");

            VersionedEntityPropertyAccessor p = iter.getProperty(VPROPERTY_MAGIC);
            out.append(Long.toString(p.getLong()));
            out.append(", iv=" + p.getInitialValue());
            out.append(", ct=" + p.getCreationTime());

            out.append(", nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

            EdgeIterator edges = iter.getIncomingEdges();

            int edgeNum = 0;
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    " + edgeNum++ + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());
                out.append(", mgc=" + edges.getProperty(EPROPERTY_MAGIC).getLong());
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    " + edgeNum++ + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());
                out.append(", mgc=" + edges.getProperty(EPROPERTY_MAGIC).getLong());
            }

            out.append("\n");
        }
        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX PRINTING TOOK: " + (now - then) + "ms");
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
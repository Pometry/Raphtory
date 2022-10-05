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
 * Simple program to confirm that if multiple properties are
 * added to a node, then they are retrieved in reverse time order.
 */
public class BitcoinTestProperties {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;
    private final int VPROPERTY_MAGIC;
    private final int EPROPERTY_MAGIC;



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

        BitcoinTestProperties it = new BitcoinTestProperties(rap);
        it.setData();
        it.dump();

        System.exit(0);
    }


    public BitcoinTestProperties(RaphtoryArrowPartition rap) {
        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");
        VPROPERTY_MAGIC = rap.getVertexPropertyId("magic_vertex");
        EPROPERTY_MAGIC = rap.getEdgePropertyId("magic_edge");

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();
    }


    public void setData() throws Exception {
        // Sets the 'magic' property to multiples of the loop counter
        // but slightly out of time & numerical order.
        // The test then checks that they're retrieved in numerical order.
        for (long i=0; i<10; ++i) {
            Vertex v = _rap.getVertex();

            long time = i*100;

            v.reset(i, i, true, time);
            v.getProperty(VPROPERTY_MAGIC).setHistory(true, time).set(i*10);
            _avpm.addVertex(v);
            v.decRefCount();

            VersionedEntityPropertyAccessor f = _rap.getVertexPropertyAccessor(VPROPERTY_MAGIC);
            f.setHistory(true, time-10).set(i*10-10);
            _avpm.addProperty(i, VPROPERTY_MAGIC, f);

            f.setHistory(true, time+10).set(i*10+10);
            _avpm.addProperty(i, VPROPERTY_MAGIC, f);

            f.setHistory(true, time-20).set(i*10-20);
            _avpm.addProperty(i, VPROPERTY_MAGIC, f);
        }



        VertexIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            System.out.println("VERTEX: " + vi.next());
            ArrowPropertyIterator pi = vi.getPropertyHistory(VPROPERTY_MAGIC);
            while (pi.hasNext()) {
                VersionedEntityPropertyAccessor h = pi.next();

                System.out.println("    " + h.getCreationTime() + ", " + h.getLong());
            }
        }
    }


    public void dump() throws Exception {
        long then = System.currentTimeMillis();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in), 16384);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            out.append(Long.toString(iter.next()));
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

                out.append("\n    " + edgeNum + " -> " + edgeId);
                out.append(", f=" + edges.getSrcVertexId());
                out.append(", t=" + edges.getDstVertexId());
                out.append(", mgc=" + edges.getProperty(EPROPERTY_MAGIC).getLong());
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.append("\n    " + edgeNum + " -> " + edgeId);
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
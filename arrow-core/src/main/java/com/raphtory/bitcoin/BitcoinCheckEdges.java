/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;


public class BitcoinCheckEdges {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;


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
        //cfg._disableBoundsCheck = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        new BitcoinCheckEdges(rap).dump();

        System.exit(0);
    }


    public BitcoinCheckEdges(RaphtoryArrowPartition rap) {
        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        _avpm.loadFiles();
        _aepm.loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");
    }


    public void dump() throws Exception {
        System.out.println(new Date() + ": BITCOIN: VERTEX EDGE CHECK STARTING:");
        long then = System.currentTimeMillis();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        LongArrayList edgeIds = new LongArrayList();
        LongArrayList dstIds = new LongArrayList();
        int nTotal = 0;
        VertexIterator.AllVerticesIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            long vertexId = vi.next();

            EdgeIterator ei = vi.getOutgoingEdges();
            edgeIds.clear();
            dstIds.clear();
            while (ei.hasNext()) {
                long edgeId = ei.next();
                edgeIds.add(edgeId);
                dstIds.add(ei.getDstVertexId());
            }

            int nFound = 0;
            int n = edgeIds.size();
            for (int i=0; i<n; ++i) {
                long dstId = dstIds.getLong(i);

                int justFound = 0;
                EdgeIterator faoei = vi.findAllOutgoingEdges(dstId);
                ++nTotal;
                while (faoei.hasNext()) {
                    ++nTotal;
                    long foundEdgeId = faoei.next();
                    long foundDstId = faoei.getDstVertexId();
                    ++nFound;
                    ++justFound;

                    int index = edgeIds.indexOf(foundEdgeId);
                    if (index==-1) {
                        System.out.println("MISSING EDGE");
                    }
                    else {
                        if (foundDstId!=dstIds.getLong(index)) {
                            System.out.println("WRONG DST");
                        }

                        edgeIds.removeLong(index);
                        dstIds.removeLong(index);
                        --i;
                        --n;
                    }
                }

                if (justFound==0) {
                    System.out.println("MISSING EDGE, but had dstId");
                }
                else if (justFound>1) {
                    System.out.println("SRC: " + vertexId + " has " + justFound + " edges to DST: " + dstId);
                }
            }
            if (nFound!=vi.getNOutgoingEdges()) {
                System.out.println("WRONG COUNT: " + nFound + " != " + vi.getNOutgoingEdges());
            }
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX PRINTING TOOK: " + (now - then) + "ms, total=" + nTotal);
    }



    private StringBuilder getVertexName(long vertexId) {
        StringBuilder name = _tmpSB;

        name.setLength(0);
        Vertex v = _avpm.getVertex(vertexId);
        StringBuilder sb = v.getField(VFIELD_ADDRESS_CHAIN).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
            v.decRefCount();
            return name;
        }

        name.append(v.getField(VFIELD_ADDRESS_CHAIN).getString());
        v.decRefCount();
        return name;
    }
}
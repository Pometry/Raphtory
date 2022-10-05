/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.*;
import java.util.Date;

/**
 * Sample program that outputs vertex details - reading the vertex ids
 * from stdin, 1 per line.
 */
public class BitcoinDumpVertices {
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

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        new BitcoinDumpVertices(rap).dump();
    }


    public BitcoinDumpVertices(RaphtoryArrowPartition rap) {
        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");

        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        //emgr.loadFiles();
        _avpm.loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");
    }


    public void dump() throws Exception {
        long then = System.currentTimeMillis();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in), 16384);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        String line;
        LongOpenHashSet items = new LongOpenHashSet();
        while ((line = br.readLine())!=null) {
            line = line.trim();
            long vertexId;

            try {
                vertexId = Long.parseLong(line);
            }
            catch (Exception e) {
                // IGNORED
                continue;
            }

            if (!items.contains(vertexId) && _avpm.isValid(vertexId)) {
                items.add(vertexId);

                out.append(line);
                out.append(", ");
                out.append(getVertexName(vertexId));
                out.newLine();
            }
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX PRINTING TOOK: " + (now - then) + "ms");
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
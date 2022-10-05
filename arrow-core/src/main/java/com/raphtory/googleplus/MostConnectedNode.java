/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Date;

/**
 * Simple program that finds the most-connected vertex
 */
public class MostConnectedNode {
    private static long THE_MAX = 0L;
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;


    public MostConnectedNode(RaphtoryArrowPartition rap) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();
    }


    public void loadFiles() throws Exception {
        long then = System.currentTimeMillis();
        System.out.println(new Date() + ": STARTING");

        _aepm.loadFiles();
        _avpm.loadFiles();

        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": GP: ARROW LOADING TOOK: " + (now-then) + "ms");
    }


    public long getChildren(LongOpenHashBigSet vertexIds, LongOpenHashBigSet children) {
        VertexIterator iter = _rap.getNewAllVerticesIterator();

        vertexIds.forEach(vertexId -> {
            iter.reset(vertexId);
            EdgeIterator ei = iter.getOutgoingEdges();
            try {
                while (ei.hasNext()) {
                    ei.next();

                    long dstVertexId = ei.getDstVertexId();

                    if (!vertexIds.contains(dstVertexId) && !children.contains(dstVertexId)) {
                        children.add(dstVertexId);
                    }
                }
            }
            catch (OutOfMemoryError e) {
                System.gc();
                System.out.println("Failed with size=" + children.size64());
                throw e;
            }
        });

        return children.size64();
    }


    public long getMostConnectedNode() {
        MutableInt maxEdges = new MutableInt(0);
        MutableLong maxEdgesVertexId = new MutableLong(0L);

        VertexIterator.MTAllVerticesManager mtState = _rap.getNewMTAllVerticesManager(RaphtoryThreadPool.THREAD_POOL);

        // Execute in parallel...
        mtState.start((pid, iter) -> {
            int max = -1;
            long id = -1L;

            while (iter.hasNext()) {
                long vertexId = iter.next();

                int nEdges = iter.getNOutgoingEdges();

                if (nEdges > max) {
                    max = nEdges;
                    id = vertexId;
                }
            }

            if (max != -1) {
                synchronized (maxEdgesVertexId) {
                    if (max > maxEdges.getValue()) {
                        maxEdges.setValue(max);
                        maxEdgesVertexId.setValue(id);
                    }
                }
            }
        });
        mtState.waitTilComplete();

        System.out.println(new Date() + ": maxEdges=" + maxEdges.getValue() + " for local vertex: " + maxEdgesVertexId.getValue());

        return maxEdgesVertexId.getValue();
    }


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
        cfg._syncIDMap = false;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        MostConnectedNode mgr = new MostConnectedNode(rap);
        mgr.loadFiles();

        long vertexId = mgr.getMostConnectedNode();
        LongOpenHashBigSet ids = new LongOpenHashBigSet();
        LongOpenHashBigSet nextDegree = new LongOpenHashBigSet();

        ids.add(vertexId);

        for (int i=0; i<5; ++i) {
            System.out.println("Degree: " + (i+1) + ", total=" + mgr.getChildren(ids, nextDegree));
            LongOpenHashBigSet tmp = ids;
            ids = nextDegree;
            nextDegree = tmp;
            nextDegree.clear();
        }
        System.exit(0);
    }
}
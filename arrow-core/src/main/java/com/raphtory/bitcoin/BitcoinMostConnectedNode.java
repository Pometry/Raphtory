/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.sql.SQLSyntaxErrorException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple program that finds the most-connected vertex
 * having the halt-flag set to true and false.
 */
public class BitcoinMostConnectedNode {
    private static long THE_MAX = 0L;
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final int VFIELD_IS_HALT;

    private final MutableInt _maxHaltEdges = new MutableInt(0);
    private final MutableLong _maxHaltEdgesVertexId = new MutableLong(0L);

    private final MutableInt _maxEdges = new MutableInt(0);
    private final MutableLong _maxEdgesVertexId = new MutableLong(0L);
    private final MutableLong _minTime = new MutableLong(Long.MAX_VALUE);
    private final MutableLong _maxTime = new MutableLong(Long.MIN_VALUE);



    public BitcoinMostConnectedNode(RaphtoryArrowPartition rap) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();

        VFIELD_IS_HALT = rap.getVertexFieldId("is_halt");
    }


    public void loadFiles() throws Exception {
        long then = System.currentTimeMillis();
        System.out.println(new Date() + ": STARTING");

        _aepm.loadFiles();
        _avpm.loadFiles();

        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: ARROW LOADING TOOK: " + (now-then) + "ms");
    }


    public void findPerMonth() {
        final long ONE_MONTH = 28L * 24L * 60L * 60L * 1000L;

        VertexIterator.MTWindowedVertexManager mgr = new VertexIterator.MTWindowedVertexManager();
        final AtomicInteger count = new AtomicInteger(0);

        for (long time = _minTime.getValue(); time < _maxTime.getValue(); time += ONE_MONTH) {
            count.set(0);

            mgr.init(_rap.getVertexMgr(), RaphtoryThreadPool.THREAD_POOL, time, time + ONE_MONTH);
            long then = System.currentTimeMillis();
            mgr.start((pid, iter) -> {
                int n = 0;
                while (iter.hasNext()) {
                    iter.next();
                    ++n;
                }

                count.addAndGet(n);

                //System.out.println("P: " + pid + " -> " + n);
            });
            mgr.waitTilComplete();

            long now = System.currentTimeMillis();
            System.out.println("From: " + new Date(time) + " took " + (now-then) + "ms, count=" + count.get());
        }

        for (long time = _minTime.getValue(); time < _maxTime.getValue(); time += ONE_MONTH) {
            _rap.takeSnapshot(time, time + ONE_MONTH);
        }
    }


    public void getMostConnectedNode() {
        VertexIterator.MTAllVerticesManager mtState = _rap.getNewMTAllVerticesManager(RaphtoryThreadPool.THREAD_POOL);

        // Execute in parallel...
        mtState.start((pid, iter) -> {
            int haltMax = -1;
            long haltId = -1L;
            long localMinTime = Long.MAX_VALUE;
            long localMaxTime = Long.MIN_VALUE;

            int max = -1;
            long id = -1L;

            while (iter.hasNext()) {
                long vertexId = iter.next();

                int nEdges = iter.getNOutgoingEdges();

                if (iter.getField(VFIELD_IS_HALT).getBoolean()) {
                    if (nEdges > haltMax) {
                        haltMax = nEdges;
                        haltId = vertexId;
                    }
                }
                else {
                    if (nEdges > max) {
                        max = nEdges;
                        id = vertexId;
                    }
                }

                EdgeIterator edgeIterator = iter.getAllEdges();
                while (edgeIterator.hasNext()) {
                    edgeIterator.next();
                    long time = edgeIterator.getCreationTime();

                    if (time < localMinTime) {
                        localMinTime = time;
                    }
                    if (time > localMaxTime) {
                        localMaxTime = time;
                    }
                }
            }

            synchronized (_minTime) {
                if (localMinTime < _minTime.getValue()) {
                    _minTime.setValue(localMinTime);
                }
            }

            synchronized (_maxTime) {
                if (localMaxTime > _maxTime.getValue()) {
                    _maxTime.setValue(localMaxTime);
                }
            }

            if (haltMax != -1) {
                synchronized (_maxHaltEdgesVertexId) {
                    if (haltMax > _maxHaltEdges.getValue()) {
                        _maxHaltEdges.setValue(haltMax);
                        _maxHaltEdgesVertexId.setValue(haltId);
                    }
                }
            }

            if (max != -1) {
                synchronized (_maxEdgesVertexId) {
                    if (max > _maxEdges.getValue()) {
                        _maxEdges.setValue(max);
                        _maxEdgesVertexId.setValue(id);
                    }
                }
            }
        });
        mtState.waitTilComplete();

        _minTime.setValue(_minTime.getValue());
        _maxTime.setValue(_maxTime.getValue());

        System.out.println(new Date() + ": maxHaltEdges=" + _maxHaltEdges.getValue() + " for local vertex: " + _maxHaltEdgesVertexId.getValue());
        System.out.println(new Date() + ": maxEdges=" + _maxEdges.getValue() + " for local vertex: " + _maxEdgesVertexId.getValue());
        System.out.println(new Date() + ": minTime=" + _minTime.getValue() + " -> " + new Date(_minTime.getValue()));
        System.out.println(new Date() + ": maxTime=" + _maxTime.getValue() + " -> " + new Date(_maxTime.getValue()));
    }


    public static void main(String[] args) throws Exception {
        String src = args[0];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new BitcoinSchema();
        cfg._arrowDir = src;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1 * 1024 * 1024;
        cfg._syncIDMap = false;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        BitcoinMostConnectedNode mgr = new BitcoinMostConnectedNode(rap);
        mgr.loadFiles();

        mgr.getMostConnectedNode();

        mgr.findPerMonth();

        System.exit(0);
    }
}
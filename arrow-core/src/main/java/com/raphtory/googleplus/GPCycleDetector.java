/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;


public class GPCycleDetector {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: GPCycleDetector ARROW_DST_DIR");
            System.err.println("Note, the dst dir should contain the edge and vertex arrow files!");
            System.exit(1);
        }

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

        System.out.println(new Date() + ": STARTING");
        long then = System.currentTimeMillis();
        rap.getVertexMgr().loadFiles();
        rap.getEdgeMgr().loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": GP: ARROW LOADING TOOK: " + (now - then) + "ms");

        //Thread.sleep(60L * 1000L);

        GPCycleDetector fdp = new GPCycleDetector(rap);
        fdp.start();

        System.exit(0);
    }


    private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);

    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;

    private final ArrayList<Future<?>> _tasks = new ArrayList<>();
    private RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;

    private final int USERID_FIELD;
    private final int MAX_DEPTH = 2;


    private final VertexIterator.MTVertexIteratorConsumer _consumer = new VertexIterator.MTVertexIteratorConsumer() {
        @Override
        public void accept(int partitionId, VertexIterator iter) {
            new IterTask().processPartition(partitionId, iter);
        }
    };


    @SuppressWarnings("unchecked")
    public GPCycleDetector(RaphtoryArrowPartition rap) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();

        USERID_FIELD = _rap.getVertexFieldId("userid");

        int nPartitions = _avpm.nPartitions();
        System.out.println("NPARTITIONS=" + nPartitions);
    }


    public void start() throws Exception {
        long then = System.currentTimeMillis();

        VertexIterator.MTAllVerticesManager mgr = _rap.getNewMTAllVerticesManager(_pool);
        mgr.start(_consumer);
        mgr.waitTilComplete();

        long now = System.currentTimeMillis();
        System.out.println("GPCD SAVE OUTPUT: " + (now - then) + "ms");
    }


    private class IterTask {
        private final LongArrayList _cyclePoints = new LongArrayList();
        private final LongArraySet _cycleMap = new LongArraySet();
        private final ObjectArrayList<VertexIterator> _tmpIterators = new ObjectArrayList<>();
        private final StringBuilder _sb = new StringBuilder();
        private int _nCycles = 0;
        private int _nItems = 0;


        public void processPartition(int partitionId, VertexIterator iter) {
            System.out.println("Processing partition: " + partitionId);
            int n = 0;

            while (iter.hasNext()) {
                long rootId = iter.next();

                if (iter.getNOutgoingEdges() > 0 && iter.getNIncomingEdges() > 0) {
                    addToCycle(rootId);
                    processVertex(iter.getOutgoingEdges(), rootId, rootId, 0);
                    popFromCycle();
                }

                ++n;
            }

            System.out.println("Processed partition: " + partitionId + ", nVertices=" + n + ", nCycles=" + _nCycles + ", nItems=" + _nItems);
        }


        private VertexIterator getTmpIterator(int depth) {
            if (_tmpIterators.size()>depth) {
                return _tmpIterators.get(depth);
            }

            VertexIterator iter = _rap.getNewAllVerticesIterator();
            _tmpIterators.add(iter);

            return iter;
        }


        private void processVertex(EdgeIterator ei, long rootId, long parentId, int depth) {
            VertexIterator tmpIterator = getTmpIterator(depth);

            while (ei.hasNext()) {
                ei.next();

                long dstVertexId = ei.getDstVertexId();
                //System.out.println("Edge: " + dstVertexId);

                if (dstVertexId == parentId) {
                    // Ignore a simple back-link
                    continue;
                }

                if (depth >= MAX_DEPTH) {
                    if (dstVertexId==rootId) {
                        addToCycle(dstVertexId);
                        outputCycle();
                        popFromCycle();

                        ++_nCycles;
                    }
                    continue;
                }

                if (depth<MAX_DEPTH && notAlreadySeen(dstVertexId)) {
                    addToCycle(dstVertexId);
                    tmpIterator.reset(dstVertexId);
                    processVertex(tmpIterator.getOutgoingEdges(), rootId, dstVertexId, depth + 1);
                    popFromCycle();

                    if ((++_nItems % (128 * 1024))==0) {
                        System.out.println(_nItems);
                    }
                }
            }
        }


        private void outputCycle() {
            if (true) return;
            _sb.setLength(0);
            int n = _cyclePoints.size();
            for (int i = 0; i < n; ++i) {
                if (i != 0) {
                    _sb.append(", ");
                }
                _sb.append(_cyclePoints.getLong(i));
            }
            System.out.println(_sb);
        }


        private void addToCycle(long vertexId) {
            _cyclePoints.push(vertexId);
        }


        private void popFromCycle() {
            _cyclePoints.popLong();
        }


        private boolean notAlreadySeen(long vertexId) {
            return !_cyclePoints.contains(vertexId);
        }
    }


    private static void outputBytes(byte[] b, BufferedOutputStream bos) throws Exception {
        bos.write(b, 0, b.length);
    }


    private static byte[] _number = new byte[64];

    private static void outputNumber(long l, BufferedOutputStream bos) throws Exception {
        byte[] number = _number;
        int offset = number.length;

        boolean negative = (l < 0L);
        if (negative) {
            l *= -1L;
        }
        do {
            int digit = (int) (l % 10);
            number[--offset] = (byte) (digit + '0');
            l /= 10;
        }
        while (l > 0);

        if (negative) {
            number[--offset] = '-';
        }
        bos.write(number, offset, number.length - offset);
    }
}
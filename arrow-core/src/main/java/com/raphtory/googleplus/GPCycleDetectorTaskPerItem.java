/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;


public class GPCycleDetectorTaskPerItem {
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

        GPCycleDetectorTaskPerItem fdp = new GPCycleDetectorTaskPerItem(rap);
        fdp.start();

        System.exit(0);
    }


    private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);

    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;

    private final ObjectArrayList<TaskRunner> _runners = new ObjectArrayList<TaskRunner>();
    private RaphtoryThreadPool _pool = new RaphtoryThreadPool(Runtime.getRuntime().availableProcessors(), 256);

    private final int USERID_FIELD;
    private final int MAX_DEPTH = 4;


    @SuppressWarnings("unchecked")
    public GPCycleDetectorTaskPerItem(RaphtoryArrowPartition rap) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();

        USERID_FIELD = _rap.getVertexFieldId("userid");

        int nPartitions = _avpm.nPartitions();
        System.out.println("NPARTITIONS=" + nPartitions);

        for (int i=0; i<1024; ++i) {
            putRunner(new TaskRunner());
        }
    }


    public void start() throws Exception {
        long then = System.currentTimeMillis();

        /*
        VertexIterator.AllVerticesIterator avi = _rap.getNewAllVerticesIterator();
        while (avi.hasNext()) {
            if (avi.getNOutgoingEdges() > 0 && avi.getNIncomingEdges() > 0) {
                _pool.submitTask(getRunner(avi.localId()));
            }

            if ((avi.localId()) % (8192)==0) System.out.println(avi.localId());
        }
        */

        _pool.submitTask(getRunner(570));

        while (!_pool.isIdle()) {
            Thread.sleep(1000L);
        }

        long now = System.currentTimeMillis();
        System.out.println("GPCD SAVE OUTPUT: " + (now - then) + "ms");
    }


    private synchronized Runnable getRunner(long rootId) {
        TaskRunner it = _runners.pop();
        it.init(rootId);
        return it;
    }


    private synchronized void putRunner(TaskRunner it) {
        _runners.push(it);
    }


    private class TaskRunner implements Runnable {
        private final LongArrayList _cyclePoints = new LongArrayList();
        private final LongArraySet _cycleMap = new LongArraySet();
        private final ObjectArrayList<VertexIterator> _tmpIterators = new ObjectArrayList<>();
        private final StringBuilder _sb = new StringBuilder();
        private int _nCycles = 0;
        private int _nItems = 0;
        private long _rootId;
        private final VertexIterator.AllVerticesIterator _iter = _rap.getNewAllVerticesIterator();


        public void init(long rootId) {
            _rootId = rootId;
        }

        @Override
        public void run() {
            //System.out.println("Processing node: " + _rootId);

            _iter.reset(_rootId);

            addToCycle(_rootId);
            processVertex(_iter, _rootId, 0);
            popFromCycle();

            System.out.println("Processed node: " + _rootId + ", nCycles=" + _nCycles + ", nItems=" + _nItems);

            _nCycles = 0;
            _nItems = 0;
            putRunner(this);
        }


        private VertexIterator getTmpIterator(int depth) {
            if (_tmpIterators.size()>depth) {
                return _tmpIterators.get(depth);
            }

            VertexIterator iter = _rap.getNewAllVerticesIterator();
            _tmpIterators.add(iter);

            return iter;
        }


        private void processVertex(VertexIterator iter, long parentId, int depth) {
            VertexIterator tmpIterator = getTmpIterator(depth);

            EdgeIterator ei = iter.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();

                long dstVertexId = ei.getDstVertexId();
                //System.out.println("Edge: " + dstVertexId);

                if (dstVertexId == parentId) {
                    // Ignore a simple back-link
                    continue;
                }

                if (depth >= MAX_DEPTH) {
                    if (dstVertexId==_rootId) {
                        addToCycle(dstVertexId);
                        outputCycle();
                        popFromCycle();

                        ++_nCycles;
                    }
                    continue;
                }

                if (notAlreadySeen(dstVertexId)) {
                    addToCycle(dstVertexId);
                    tmpIterator.reset(dstVertexId);
                    processVertex(tmpIterator, dstVertexId, depth + 1);
                    popFromCycle();

                    if ((++_nItems % (128 * 1024))==0) {
                        //System.out.println(_nItems);
                    }
                }
            }
        }


        private void outputCycle() {
            if (false) return;
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
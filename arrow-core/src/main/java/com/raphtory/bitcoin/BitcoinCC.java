/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample implementation of a connected components algorithm -
 * operates on the entire data-set.
 */
public class BitcoinCC {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: BitcoinCC ARROW_DST_DIR");
            System.err.println("Note, the dst dir should contain the edge and vertex arrow files!");
            System.exit(1);
        }

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
        rap.getVertexMgr().loadFiles();
        rap.getEdgeMgr().loadFiles();
        BitcoinCC mgr = new BitcoinCC(rap, 100);
        mgr.start();

        System.exit(0);
    }


    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final ArrayList<Future<?>> _tasks = new ArrayList<>();
    private final RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;
    private final int _nIterations;
    private final VertexIterator.MTAllVerticesManager _mtState;
    private final Long2LongOpenHashMap[] _stepStates;
    private final LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];
    private final AtomicLong _nChanges = new AtomicLong(0);


    public BitcoinCC(RaphtoryArrowPartition rap, int nIterations) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();

        _nIterations = nIterations;

        _mtState = _rap.getNewMTAllVerticesManager(_pool);

        int nPartitions = _avpm.nPartitions();
        _stepStates = new Long2LongOpenHashMap[nPartitions];

        _changes2[0] = new LongOpenHashSet[nPartitions];
        _changes2[1] = new LongOpenHashSet[nPartitions];

        for (int i=0; i<nPartitions; ++i) {
            _changes2[0][i] = new LongOpenHashSet(8192);
            _changes2[1][i] = new LongOpenHashSet(8192);
        }
    }


    /* Starts the CC algorithm */
    public void start() throws Exception {
        long then = System.currentTimeMillis();

        step();
        long now = System.currentTimeMillis();
        System.out.println("STEP: " + _nChanges.get() + " took " + (now-then) + "ms");

        long prevNow = now;

        for (int i=1; i<=_nIterations; ++i) {
            _nChanges.set(0);
            if (!iterate(i)) {
                break;
            }

            now = System.currentTimeMillis();
            System.out.println("ITERATION: " + i + " , changes=" + _nChanges.get() + " took " + (now-prevNow) + "ms");
            prevNow = now;
        }

        now = System.currentTimeMillis();
        System.out.println("FINISHED: " + (now - then) + "ms");

        then = System.currentTimeMillis();
        createCCOutput();
        dump();
        now = System.currentTimeMillis();

        System.out.println("MT2CC SAVE OUTPUT: " + (now-then) + "ms");
    }


    // Generates the CC output as a file containing group-ids and their sizes
    private void createCCOutput() {
        File dir = new File("ccoutput");
        if (!dir.exists()) {
            dir.mkdir();
        }

        long then = System.currentTimeMillis();

        int nPartitions = _avpm.nPartitions();
        Long2LongOpenHashMap[] maps = new Long2LongOpenHashMap[nPartitions];
        for (int i=0; i<nPartitions; ++i) {
            maps[i] = new Long2LongOpenHashMap();
            maps[i].defaultReturnValue(-1L);
        }

        ArrayList<Future<?>> tasks = new ArrayList<>();

        for (int i=0; i<nPartitions; ++i) {
            Long2LongOpenHashMap src = _stepStates[i];
            Long2LongOpenHashMap dst = maps[i];

            // Calculate the groupings in each partition (src) in parallel
            tasks.add(_pool.submitTask(() -> {
                src.values().forEach(componentGroup -> {
                    long count = dst.putIfAbsent(componentGroup, 1L);
                    if (count != -1L) {
                        dst.put(componentGroup, count + 1);
                    }
                });
            }));
        }

        _pool.waitTilComplete(tasks);
        tasks.clear();

        Long2LongOpenHashMap results = new Long2LongOpenHashMap();
        results.defaultReturnValue(-1L);

        // Combine the per-partition groupings...
        for (int i=0; i<nPartitions; ++i) {
            Long2LongOpenHashMap src = maps[i];
            src.long2LongEntrySet().fastForEach(e -> {
                long componentGroup = e.getLongKey();
                long count = e.getLongValue();

                long prevCount = results.putIfAbsent(componentGroup, count);
                if (prevCount!=-1L) {
                    results.put(componentGroup, count+prevCount);
                }
            });
        }

        long now = System.currentTimeMillis();
        System.out.println("RESULTS COLLATION: " + (now-then) + "ms");

        // Create the output file
        try {
            File outFile = new File("ccoutput", "results.csv");
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
            byte[] COMMA = ", ".getBytes();

            bw.write("group-size, group-id\n".getBytes());

            results.long2LongEntrySet().fastForEach(e -> {
                try {
                    outputNumber(e.getLongValue(), bw);
                    outputBytes(COMMA, bw);
                    outputNumber(e.getLongKey(), bw);
                    bw.write((byte) '\n');
                }
                catch (Exception ex) {
                }
            });

            bw.close();
            fos.close();
        }
        catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.out);
        }
    }


    // Outputs raw files for debugging
    private void dump() throws Exception {
        File dir = new File("ccoutput");
        if (!dir.exists()) {
            dir.mkdir();
        }

        ArrayList<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < _stepStates.length; ++i) {
            final int ii = i;
            tasks.add(_pool.submitTask(() -> dumpRawFile(ii)));
        }

        _pool.waitTilComplete(tasks);
        tasks.clear();
    }


    // Output a raw file for a single partition for debugging
    private void dumpRawFile(int partId) {
        File outFile = new File("ccoutput", "CC.output." + partId + ".csv");

        try {
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
            byte[] COMMA = ", ".getBytes();

            bw.write("vertex-id, group-id\n".getBytes());

            Long2LongOpenHashMap map = _stepStates[partId];
            map.long2LongEntrySet().fastForEach(e -> {
                try {
                    outputNumber(e.getLongKey(), bw);
                    outputBytes(COMMA, bw);
                    outputNumber(e.getLongValue(), bw);
                    bw.write((byte) '\n');
                }
                catch (Exception ex) {
                }
            });

            bw.close();
            fos.close();
        }
        catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.out);
        }
    }


    public void waitTilComplete() {
        _pool.waitTilComplete(_tasks);
    }


    public void cancel() {
        _pool.cancel(_tasks);
    }


    /**
     *  Used to perform a single CC iteration, operating on a single
     * arrow file, in it's own thread.
     */
    private class IterateTask implements Runnable, LongConsumer {
        private VertexIterator _iter;
        private final int _iteration;
        private final int _partitionId;
        private final Long2LongOpenHashMap _state;

        IterateTask(int partitionId, int iteration) {
            _partitionId = partitionId;
            _iteration = iteration;
            _state = _stepStates[partitionId];
        }


        @Override
        public void run() {
            _iter = _rap.getNewAllVerticesIterator();

            // Iterate over the previous list of changed nodes
            int changeIndex = (_iteration-1) & 0x1;
            _changes2[changeIndex][_partitionId].forEach(this);
        }


        @Override
        public void accept(long value) {
            processVertex(value);
        }


        // Process a single vertex and it's destinations
        private void processVertex(long vertexId) {
            long myValue = _state.get(vertexId);

            _iter.reset(vertexId);

            EdgeIterator wves = _iter.getIncomingEdges();
            while (wves.hasNext()) {
                wves.next();

                boolean isLocal = wves.isSrcVertexLocal();
                if (isLocal) {
                    long srcVertexId = wves.getSrcVertexId();
                    setState(_iteration, srcVertexId, myValue);
                }
            }

            wves = _iter.getOutgoingEdges();
            while (wves.hasNext()) {
                wves.next();

                boolean isLocal = wves.isDstVertexLocal();
                if (isLocal) {
                    long dstVertexId = wves.getDstVertexId();
                    setState(_iteration, dstVertexId, myValue);
                }
            }
        }
    }


    // Set the CC state on a single vertex
    private boolean setState(int iteration, long vertexId, long newValue) {
        int partId = _avpm.getPartitionId(vertexId);
        Long2LongOpenHashMap state = _stepStates[partId];

        synchronized (state) {
            long oldValue = state.get(vertexId);
            if (oldValue == -1L || oldValue > newValue) {
                state.put(vertexId, newValue);

                int changeId = iteration & 0x01;
                _changes2[changeId][partId].add(vertexId);

                _nChanges.getAndIncrement();
                return true;
            }

            return false;
        }
    }


    // Perform an iteration across all arrow files,
    // each file being processed in its own thread.
    private boolean iterate(int iteration) {
        _tasks.clear();
        int nPartitions = _avpm.nPartitions();

        synchronized (_tasks) {
            final int nextChanges = iteration & 0x1;
            for (int i=0; i<nPartitions; ++i) {
                final int ii = i;
                _tasks.add(_pool.submitTask(() -> _changes2[nextChanges][ii].clear()));
            }
            waitTilComplete();

            _tasks.clear();
            for (int i=0; i<nPartitions; ++i) {
                _tasks.add(_pool.submitTask(new IterateTask(i, iteration)));
            }
        }

        waitTilComplete();
        _tasks.clear();

        return _nChanges.get()!=0;
    }


    // Perform the initial CC processing - this is distributed
    // as 1 arrow file per thread
    private void step() {
        long then = System.currentTimeMillis();

        int nPartitions = _avpm.nPartitions();
        for (int i=0; i<nPartitions; ++i) {
            _stepStates[i] = new Long2LongOpenHashMap(16384);
        }

        _mtState.start(this::stepFn);
        _mtState.waitTilComplete();

        long now = System.currentTimeMillis();
        System.out.println("MTAV STEP: took: " + (now-then) +"ms");
    }


    // Perform the initial CC processing on a single arrow
    // partition (file)
    private void stepFn(int partitionId, VertexIterator iter) {
        while (iter.hasNext()) {
            long vertexId = iter.next();
            long globalVertexId = iter.getGlobalVertexId();

            setState(0, vertexId, globalVertexId);

            EdgeIterator wves = iter.getIncomingEdges();
            while (wves.hasNext()) {
                wves.next();

                boolean isLocal = wves.isSrcVertexLocal();
                if (isLocal) {
                    long srcVertexId = wves.getSrcVertexId();
                    setState(0, srcVertexId, globalVertexId);
                }
            }

            wves = iter.getOutgoingEdges();
            while (wves.hasNext()) {
                wves.next();

                boolean isLocal = wves.isDstVertexLocal();
                if (isLocal) {
                    long dstVertexId = wves.getDstVertexId();
                    setState(0, dstVertexId, globalVertexId);
                }
            }
        }
    }


    private static void outputBytes(byte[] b, BufferedOutputStream bos) throws  Exception {
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
            int digit = (int)(l % 10);
            number[--offset] = (byte)(digit + '0');
            l /= 10;
        }
        while (l>0);

        if (negative) {
            number[--offset] = '-';
        }
        bos.write(number, offset, number.length-offset);
    }
}
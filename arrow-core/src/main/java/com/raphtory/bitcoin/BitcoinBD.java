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
 * Sample implementation of a binary diffusion algorithm.
 */
public class BitcoinBD {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: BitcoinBD ARROW_DST_DIR [root-vertex-id|-n-to-infect]");
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
        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        rap.getVertexMgr().loadFiles();
        rap.getEdgeMgr().loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");

        VertexPartitionManager avpm = rap.getVertexMgr();
        int nPartitions = avpm.nPartitions();
        long THE_MAX = (long)nPartitions * (long)avpm.PARTITION_SIZE;

        LongOpenHashSet[] roots;
        if (args.length==1) {
            roots = null;
        }
        else if (args[1].startsWith("-")) {
            int nRoots = Integer.parseInt(args[1].substring(1));
            roots = new LongOpenHashSet[nPartitions];
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for (int i=0; i<nRoots; ++i) {
                long vertexId = rng.nextLong(THE_MAX);
                int pId = avpm.getPartitionId(vertexId);

                LongOpenHashSet set = roots[pId];
                if (set==null) {
                    set = new LongOpenHashSet(1024);
                    roots[pId] = set;
                }

                set.add(vertexId);
            }
        }
        else {
            roots = new LongOpenHashSet[nPartitions];
            long vertexId = Long.parseLong(args[1]);
            int pId = avpm.getPartitionId(vertexId);

            LongOpenHashSet set = roots[pId];
            if (set==null) {
                set = new LongOpenHashSet(1024);
                roots[pId] = set;
            }

            set.add(vertexId);
        }

        new MT2BinaryDiffusion(rap, 100, roots, false, true).start();

        System.exit(0);
    }



    private static class MT2BinaryDiffusion {
        private RaphtoryArrowPartition _rap;
        private VertexPartitionManager _avpm;
        private EdgePartitionManager _aepm;
        int _nIterations;
        VertexIterator.MTAllVerticesManager _mtState;
        private final ArrayList<Future<?>> _tasks = new ArrayList<>();
        private Long2BooleanOpenHashMap[] _stepStates;
        private Long2LongOpenHashMap[] _stepTimes;
        private LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];
        private AtomicLong _nChanges = new AtomicLong(0);
        private final boolean _reinfect;
        private final boolean _useHaltFlag;
        private final LongOpenHashSet[] _infectedSet;
        private RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;
        private final long THE_MAX;
        private final int VFIELD_IS_HALT;


        public MT2BinaryDiffusion(RaphtoryArrowPartition rap, int nIterations,
                                  LongOpenHashSet[] infectedSet, boolean reinfect, boolean useHaltFlag) {
            _rap = rap;
            VFIELD_IS_HALT = rap.getVertexFieldId("is_halt");

            _avpm = rap.getVertexMgr();
            _aepm = rap.getEdgeMgr();
            _nIterations = nIterations;

            _infectedSet = infectedSet;
            _reinfect = reinfect;
            _useHaltFlag = useHaltFlag;

            _mtState = _rap.getNewMTAllVerticesManager(_pool);

            THE_MAX = (long)_avpm.nPartitions() * (long) _avpm.PARTITION_SIZE;
        }


        /* Runs the binary diffusion algorithm... */
        public void start() throws Exception {
            long then = System.currentTimeMillis();

            int nPartitions = _avpm.nPartitions();
            System.out.println("NPARTITIONS=" + nPartitions);

            _stepStates = new Long2BooleanOpenHashMap[nPartitions];
            _stepTimes = new Long2LongOpenHashMap[nPartitions];

            _changes2[0] = new LongOpenHashSet[nPartitions];
            _changes2[1] = new LongOpenHashSet[nPartitions];

            for (int i=0; i<nPartitions; ++i) {
                _changes2[0][i] = new LongOpenHashSet(16384);
                _changes2[1][i] = new LongOpenHashSet(16384);
                _stepTimes[i] = new Long2LongOpenHashMap(16384);
            }

            step();
            //dumpStep(0);

            long now = System.currentTimeMillis();
            System.out.println("STEP: " + _nChanges.get() + " took " + (now-then) + "ms");

            long prevNow = now;

            for (int i=1; i<=_nIterations; ++i) {
                _nChanges.set(0);

                try {
                    if (!iterate(i)) {
                        //dumpStep(i);
                        break;
                    }
                }
                finally {
                    now = System.currentTimeMillis();
                    System.out.println("ITERATION: " + i + " , changes=" + _nChanges.get() + " took " + (now-prevNow) + "ms");
                    prevNow = now;
                }

                //dumpStep(i);
            }

            now = System.currentTimeMillis();
            System.out.println("FINISHED: " + (now - then) + "ms");

            then = System.currentTimeMillis();
            dump();
            now = System.currentTimeMillis();
            System.out.println("MT2BD SAVE OUTPUT: " + (now-then) + "ms");
        }


        // Outputs the BD data into separate files (1 per arrow partition)
        private void dump() throws Exception {
            File dir = new File("bdoutput");
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


        // Outputs the BD data for a single arrow partition
        private void dumpRawFile(int partId) {
            File outFile = new File("bdoutput", "BD.output" + partId + ".csv");

            try {
                FileOutputStream fos = new FileOutputStream(outFile);
                BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
                byte[] BTRUE = ", true\n".getBytes();
                byte[] BFALSE = ", false\n".getBytes();

                bw.write("vertex-id, infected\n".getBytes());

                Long2BooleanOpenHashMap map = _stepStates[partId];
                long localId = partId * _avpm.PARTITION_SIZE;
                long maxId = localId + _avpm.PARTITION_SIZE;
                maxId = Math.min(THE_MAX, maxId);

                for (long l=localId; l<maxId; ++l) {
                    if (_avpm.isValid(l)) {
                        outputNumber(l, bw);

                        if (map.get(l)) {
                            outputBytes(BTRUE, bw);
                        }
                        else {
                            outputBytes(BFALSE, bw);
                        }
                    }
                }

                bw.close();
                fos.close();
            }
            catch (Exception e) {
                System.out.println("Exception: " + e);
                e.printStackTrace(System.out);
            }
        }


        // Outputs intermediary data for debugging purposes
        private void dumpStep(int iteration) throws Exception {
            File dir = new File("bdoutput");
            if (!dir.exists()) {
                dir.mkdir();
            }

            ArrayList<Future<?>> tasks = new ArrayList<>();
            for (int i=0; i<_stepStates.length; ++i) {
                final int ii = i;
                tasks.add(_pool.submitTask(() -> dumpRawFile(ii, iteration)));
            }

            _pool.waitTilComplete(tasks);
            tasks.clear();
        }


        // Outputs intermediary data for a single arrow paritition for debugging purposes
        private void dumpRawFile(int partId, int iteration) {
            File outFile = new File("bdoutput", "BD.output" + partId + ".csv." + iteration);

            try {
                FileOutputStream fos = new FileOutputStream(outFile);
                BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
                byte[] BTRUE = ", true\n".getBytes();
                byte[] BFALSE = ", false\n".getBytes();

                bw.write("vertex-id, infected\n".getBytes());

                Long2BooleanOpenHashMap map = _stepStates[partId];
                long localId = partId * _avpm.PARTITION_SIZE;
                long maxId = localId + _avpm.PARTITION_SIZE;
                maxId = Math.min(THE_MAX, maxId);

                for (long l=localId; l<maxId; ++l) {
                    if (_avpm.isValid(l)) {
                        outputNumber(l, bw);

                        if (map.get(l)) {
                            outputBytes(BTRUE, bw);
                        }
                        else {
                            outputBytes(BFALSE, bw);
                        }
                    }
                }

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
         * This class is used to process each iteration of infection after
         * the initial seeding. One instance per arrow partition.
         * Each arrow partition is iterated over in its own thread.
         */
        private class IterateTask implements Runnable, LongConsumer {
            private VertexIterator _vertexIterator;
            private int _iteration;
            private int _partitionId;
            private ThreadLocalRandom _rng;
            private EntityFieldAccessor[] _tmpVertexFields;


            IterateTask(int partitionId, int iteration) {
                _partitionId = partitionId;
                _iteration = iteration;
            }


            @Override
            public void run() {
                _rng = ThreadLocalRandom.current();
                _vertexIterator = _rap.getNewAllVerticesIterator();
                _tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

                int changeIndex = (_iteration-1) & 0x1;
                _changes2[changeIndex][_partitionId].forEach(this);
            }


            @Override
            public void accept(long value) {
                processVertex(value);
            }


            private void processVertex(long vertexId) {
                _vertexIterator.reset(vertexId);
                long time = _vertexIterator.getCreationTime();

                boolean thisNodeIsHalt = _vertexIterator.getField(VFIELD_IS_HALT).getBoolean();
                if (infect(_iteration, thisNodeIsHalt, vertexId, time)) {
                    EdgeIterator wves = _vertexIterator.getOutgoingEdges();
                    while (wves.hasNext()) {
                        wves.next();

                        if (_rng.nextBoolean()) {
                            if (wves.isDstVertexLocal()) {
                                long dstVertexId = wves.getDstVertexId();
                                boolean dstIsHalt = _avpm.getField(dstVertexId, VFIELD_IS_HALT, _tmpVertexFields).getBoolean();
                                infect(_iteration, dstIsHalt, dstVertexId, wves.getCreationTime());
                            }
                            else {
                                // Placeholder for remote vertices: DO SOMETHING ELSE
                            }
                        }
                    }
                }
            }
        }


        // Infects a single vertex
        private boolean infect(int iteration, boolean isHalt, long vertexId, long infectionTime) {
            int partId = _avpm.getPartitionId(vertexId);
            Long2BooleanOpenHashMap state = _stepStates[partId];

            synchronized (state) {
                boolean prevInfected = state.get(vertexId);
                if (!prevInfected) {
                    state.put(vertexId, true);
                    _stepTimes[partId].put(vertexId, infectionTime);

                    int changeId = iteration & 0x01;
                    _changes2[changeId][partId].add(vertexId);

                    _nChanges.getAndIncrement();
                    return !isHalt;
                }

                return _reinfect && !isHalt;
            }
        }


        // Performs a BD iteration over all arrow partitions,
        // each in it's own thread.
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


        // Performs the initial infection step,
        // each arrow partition being handled in parallel.
        private void step() {
            long then = System.currentTimeMillis();

            int nPartitions = _avpm.nPartitions();
            for (int i=0; i<nPartitions; ++i) {
                _stepStates[i] = new Long2BooleanOpenHashMap(16384);
            }

            if (_infectedSet!=null) {
                // Seed using the infected-set
                synchronized (_tasks) {
                    _tasks.clear();

                    for (int i=0; i<nPartitions; ++i) {
                        _tasks.add(_pool.submitTask(new SeedTask(i)));
                    }
                }

                waitTilComplete();
                _tasks.clear();
            }
            else {
                // Seed using the entire data-set
                _mtState.start(this::stepFn);
                _mtState.waitTilComplete();
            }

            long now = System.currentTimeMillis();
            System.out.println("STEP: took: " + (now-then) +"ms");
        }


        /* This class performs the initial seeding of a list of infected vertices. */
        private class SeedTask implements Runnable, LongConsumer {
            private final VertexIterator _iter;
            private final int _partitionId;
            private ThreadLocalRandom _rng;
            private EntityFieldAccessor[] _tmpVertexFieldAccessors;

            SeedTask(int partitionId) {
                _partitionId = partitionId;
                _iter = _rap.getNewAllVerticesIterator();
            }


            @Override
            public void run() {
                _rng = ThreadLocalRandom.current();
                _tmpVertexFieldAccessors = _rap.getTmpVertexEntityFieldAccessors();

                if (_infectedSet[_partitionId]!=null) {
                    _infectedSet[_partitionId].forEach(this);
                }
            }


            @Override
            public void accept(long value) {
                seedVertex(value);
            }


            // Seed a single vertex
            private void seedVertex(long vertexId) {
                _iter.reset(vertexId);

                if (_iter.isValid()) {
                    EdgeIterator wves = _iter.getOutgoingEdges();
                    infectVertex(vertexId, _iter.getField(VFIELD_IS_HALT).getBoolean(), _iter.getCreationTime(), wves, _rng, _tmpVertexFieldAccessors);
                }
                else {
                    //System.out.println("Not seeding: " + vertexId);
                }
            }
        }


        // This function seeds all the vertices in an arrow partition
        private void stepFn(int partitionId, VertexIterator iter) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            EntityFieldAccessor[] tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

            while (iter.hasNext()) {
                long localId = iter.next();

                if (rng.nextBoolean()) {
                    infectVertex(localId, iter.getField(VFIELD_IS_HALT).getBoolean(), iter.getCreationTime(), iter.getOutgoingEdges(), rng, tmpVertexFields);
                }
            }
        }


        // Main binary diffusion infection algorithm.
        private void infectVertex(long vertexId, boolean isHalt, long infectionTime, EdgeIterator edgeIterator, ThreadLocalRandom rng, EntityFieldAccessor[] tmpVertexFields) {
            if (infect(0, isHalt, vertexId, infectionTime)) {
                while (edgeIterator.hasNext()) {
                    edgeIterator.next();

                    if (rng.nextBoolean()) {
                        if (edgeIterator.isDstVertexLocal()) {
                            long dstVertexId = edgeIterator.getDstVertexId();

                            boolean dstIsHalt = _avpm.getField(dstVertexId, VFIELD_IS_HALT, tmpVertexFields).getBoolean();
                            infect(0, dstIsHalt, dstVertexId, edgeIterator.getCreationTime());
                        }
                        else {
                            // DO SOMETHING ELSE
                        }
                    }
                }
            }
            else {
                //System.out.println("NOT INFECTING CHILDREN OF: " + vertexId);;
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
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample implementation of a forward-diffusion algorithm.
 * This version does not maintain any paths.
 */
public class BitcoinFD {
    public static void main(String[] args) throws Exception {
        if (args==null || args.length<2) {
            System.err.println("Usage: BitcoinFD ARROW_DST_DIR '2022-04-04 12:05:00' [v1 v2 v3...]");
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

        int nPartitions = rap.getVertexMgr().nPartitions();
        long THE_MAX = (long)nPartitions * (long) rap.getVertexMgr().PARTITION_SIZE;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long initialInfectionTime = LocalDateTime.parse(args[1], formatter).toEpochSecond(ZoneOffset.UTC);
        LongOpenHashSet[] roots = new LongOpenHashSet[nPartitions];

        if (args.length>=3) {
            // Process supplied roots
            ArrayList<Long> rootList = new ArrayList<>();

            for (int i=2; i<args.length; ++i) {
                rootList.add(Long.parseLong(args[i]));
            }

            for (long vertexId: rootList) {
                int pId = rap.getVertexMgr().getPartitionId(vertexId);

                LongOpenHashSet set = roots[pId];
                if (set==null) {
                    set = new LongOpenHashSet(1024);
                    roots[pId] = set;
                }

                set.add(vertexId);
            }
        }
        else {
            System.out.println("GENERATING RANDOM ROOTS...");
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for (int i=0; i<30_000_000; ++i) {
                long vertexId = rng.nextLong(THE_MAX);
                int pId = rap.getVertexMgr().getPartitionId(vertexId);

                LongOpenHashSet set = roots[pId];
                if (set==null) {
                    set = new LongOpenHashSet(1024);
                    roots[pId] = set;
                }

                set.add(vertexId);
            }
        }

        new MT2ForwardDiffusion(rap, Long.MIN_VALUE, Long.MAX_VALUE, 1000, roots, initialInfectionTime, true).start();

        System.exit(0);
    }


    private static class MT2ForwardDiffusion {
        private VertexPartitionManager _avpm;
        long _minTime;
        long _maxTime;
        int _nIterations;
        VertexIterator.MTAllVerticesManager _mtState;
        private final ArrayList<Future<?>> _tasks = new ArrayList<>();
        private AtomicLong _nChanges = new AtomicLong(0);
        private final boolean _useHaltFlag;
        private final LongOpenHashSet[] _infectedSet;
        private final long _initialInfectionTime;
        private RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;
        private final long THE_MAX;
        private final int VFIELD_IS_HALT;
        private final int VFIELD_TXN_HASH;
        private final int VFIELD_ADDR_CHAIN;
        private final RaphtoryArrowPartition _rap;

        // These fields are accessed in a multi-threaded manner
        // so access to them needs to be controlled.
        private final Long2BooleanOpenHashMap[] _stepStates;
        private final Long2LongOpenHashMap[] _stepTimes;
        private final LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];



        public MT2ForwardDiffusion(RaphtoryArrowPartition rap, long minTime, long maxTime, int nIterations,
                                   LongOpenHashSet[] infectedSet, long initialInfectionTime, boolean useHaltFlag) {
            _rap = rap;
            VFIELD_IS_HALT = rap.getVertexFieldId("is_halt");
            VFIELD_TXN_HASH = rap.getVertexFieldId("transaction_hash");
            VFIELD_ADDR_CHAIN = rap.getVertexFieldId("address_chain");

            _avpm = rap.getVertexMgr();
            _minTime = minTime;
            _maxTime = maxTime;
            _nIterations = nIterations;

            _infectedSet = infectedSet;
            _initialInfectionTime = initialInfectionTime;
            _useHaltFlag = useHaltFlag;

            _mtState = _rap.getNewMTAllVerticesManager(_pool);

            int nPartitions = rap.getVertexMgr().nPartitions();
            System.out.println("NPARTITIONS=" + nPartitions);

            THE_MAX = (long)nPartitions * (long) _avpm.PARTITION_SIZE;

            _stepStates = new Long2BooleanOpenHashMap[nPartitions];
            _stepTimes = new Long2LongOpenHashMap[nPartitions];

            _changes2[0] = new LongOpenHashSet[nPartitions];
            _changes2[1] = new LongOpenHashSet[nPartitions];

            for (int i=0; i<nPartitions; ++i) {
                _stepStates[i] = new Long2BooleanOpenHashMap(16384);
                _stepTimes[i] = new Long2LongOpenHashMap(_avpm.PARTITION_SIZE);
                _stepTimes[i].defaultReturnValue(-1L);

                _changes2[0][i] = new LongOpenHashSet(_avpm.PARTITION_SIZE);
                _changes2[1][i] = new LongOpenHashSet(_avpm.PARTITION_SIZE);
            }
        }


        public void start() throws Exception {
            long then = System.currentTimeMillis();

            int nPartitions = _avpm.nPartitions();
            System.out.println("NPARTITIONS=" + nPartitions);

            step();
            //dumpStep(0);

            long now = System.currentTimeMillis();
            System.out.println("STEP: " + _nChanges.get() + " took " + (now-then) + "ms");

            long prevNow = now;

            for (int i=1; i<=_nIterations; ++i) {
                _nChanges.set(0);
                if (!iterate(i)) {
                    //dumpStep(i);
                    break;
                }

                //dumpStep(i);

                now = System.currentTimeMillis();
                System.out.println("ITERATION: " + i + " , changes=" + _nChanges.get() + " took " + (now-prevNow) + "ms");
                prevNow = now;
            }

            now = System.currentTimeMillis();
            System.out.println("FINISHED: " + (now - then) + "ms");

            then = System.currentTimeMillis();
            dump();
            now = System.currentTimeMillis();
            System.out.println("MT2BD SAVE OUTPUT: " + (now-then) + "ms");
        }


        // Outputs the raw files...
        private void dump() throws Exception {
            File dir = new File("fdoutput");
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


        // Outputs a single file...
        private void dumpRawFile(int partId) {
            File outFile = new File("fdoutput", "FD.output" + partId + ".csv");

            try {
                FileOutputStream fos = new FileOutputStream(outFile);
                BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
                byte[] BTRUE = ", true\n".getBytes();
                byte[] BFALSE = ", false\n".getBytes();
                byte[] COMMA = ", ".getBytes();

                bw.write("name, vertex-id, infected\n".getBytes());

                Long2BooleanOpenHashMap finalState = _stepStates[partId];
                long localId = partId * _avpm.PARTITION_SIZE;
                long maxId = localId + _avpm.PARTITION_SIZE;
                maxId = Math.min(THE_MAX, maxId);

                // Get somewhere to store the field values...
                EntityFieldAccessor[] tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

                for (long l=localId; l<maxId; ++l) {
                    if (_avpm.isValid(l)) {
                        StringBuilder name = getVertexName(l, tmpVertexFields);

                        outputBytes(name.toString().getBytes(), bw); // XXX GARBAGE
                        outputBytes(COMMA, bw);

                        outputNumber(l, bw);

                        if (finalState.get(l)) {
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


        // Outputs the raw files at the end of each step...
        private void dumpStep(int iteration) throws Exception {
            File dir = new File("fdoutput");
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


        /* Outputs the path entries for a single arrow partition */
        private void dumpRawFile(int partId, int iteration) {
            File outFile = new File("fdoutput", "FD.output" + partId + ".csv." + iteration);

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


        /**
         * This class is used to process each iteration of infection after
         * the initial seeding. One instance per arrow partition.
         * Each arrow partition is iterated over in its own thread.
         */
        private class IterateTask implements Runnable, LongConsumer {
            private VertexIterator myIter;
            private int _iteration;
            private int _partitionId;
            private EntityFieldAccessor[] _tmpVertexFields;

            IterateTask(int partitionId, int iteration) {
                _partitionId = partitionId;
                _iteration = iteration;
            }


            /* Iterates over the infected vertices in this partition */
            @Override
            public void run() {
                myIter = _rap.getNewAllVerticesIterator();
                _tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

                // Iterate over the change-list from the PREVIOUS iteration
                int changeIndex = (_iteration-1) & 0x1;
                _changes2[changeIndex][_partitionId].forEach(this);
            }


            @Override
            public void accept(long value) {
                processVertex(value);
            }


            /* Processes a single vertex */
            private void processVertex(long vertexId) {
                Long2BooleanOpenHashMap state = _stepStates[_partitionId];

                long infectionTime;

                synchronized (state) {
                    infectionTime = _stepTimes[_partitionId].get(vertexId);
                }

                // Move to this vertex...
                myIter.reset(vertexId);
                if (!_useHaltFlag || !myIter.getField(VFIELD_IS_HALT).getBoolean()) {
                    //System.out.println("Infecting children of: " + getVertexName(vertexId));

                    EdgeIterator wves = myIter.getOutgoingEdges();
                    while (wves.hasNext()) {
                        wves.next();

                        if (wves.isDstVertexLocal()) {
                            if (wves.getCreationTime()>=infectionTime) {
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
                else {
                    //System.out.println("NOT Infecting children of: " + getVertexName(vertexId));
                }
            }
        }


        // Main infection algorithm
        private boolean infect(int iteration, boolean isHalt, long vertexId, long infectionTime) {
            int partId = _avpm.getPartitionId(vertexId);
            Long2BooleanOpenHashMap state = _stepStates[partId];
            int changeId = iteration & 0x01;

            synchronized (state) {
                boolean prevInfected = state.get(vertexId);

                if (isHalt) {
                    if (!prevInfected) {
                        state.put(vertexId, true);
                        _stepTimes[partId].put(vertexId, infectionTime);
                        _changes2[changeId][partId].add(vertexId);
                        _nChanges.getAndIncrement();
                        //System.out.println("Infected halt vertex: " + getVertexName(vertexId));
                    }

                    //System.out.println("NOT Infecting halt vertex: " + getVertexName(vertexId));
                    return false;
                }


                if (!prevInfected) {
                    state.put(vertexId, true);
                    _stepTimes[partId].put(vertexId, infectionTime);
                    _changes2[changeId][partId].add(vertexId);
                    _nChanges.getAndIncrement();

                    //System.out.println("Infecting vertex: " + getVertexName(vertexId) + " at " + new Date(infectionTime*1000L));
                    return true;
                }
                else if (_stepTimes[partId].get(vertexId)>infectionTime) {
                    _stepTimes[partId].put(vertexId, infectionTime);
                    _changes2[changeId][partId].add(vertexId);
                    _nChanges.getAndIncrement();

                    //System.out.println("Reinfecting vertex: " + getVertexName(vertexId) + " at " + new Date(infectionTime*1000L));
                    return true;
                }

                //System.out.println("Not infecting " + getVertexName(vertexId) + " as time is too low: infectionTime=" + new Date(infectionTime*1000L) + ", previousluInfected=" + new Date(_stepTimes[partId].get(vertexId)*1000L));
                return false;
            }
        }


        // Performs an iteration of infection.
        // Fires off 1 task per arrow partition
        private boolean iterate(int iteration) {
            _tasks.clear();
            int nPartitions = _avpm.nPartitions();

            synchronized (_tasks) {
                // Clear the change-set to be used for the "next" iteration
                final int nextChanges = iteration & 0x1;
                for (int i=0; i<nPartitions; ++i) {
                    final int ii = i;
                    _tasks.add(_pool.submitTask(() -> _changes2[nextChanges][ii].clear()));
                }
                waitTilComplete();

                // Process the change-set from the "last" iteration
                _tasks.clear();
                for (int i=0; i<nPartitions; ++i) {
                    _tasks.add(_pool.submitTask(new IterateTask(i, iteration)));
                }
            }

            waitTilComplete();
            _tasks.clear();

            return _nChanges.get()!=0;
        }


        /* Initial setup step */
        private void step() {
            long then = System.currentTimeMillis();

            int nPartitions = _avpm.nPartitions();

            synchronized (_tasks) {
                _tasks.clear();

                for (int i=0; i<nPartitions; ++i) {
                    _tasks.add(_pool.submitTask(new SeedTask(i)));
                }
            }

            waitTilComplete();
            _tasks.clear();

            long now = System.currentTimeMillis();
            System.out.println("MTAV STEP: took: " + (now-then) +"ms");
        }


        /* Seeds the root vertices and performs the initial set of infections
         * 1 thread per partition...
         */
        private class SeedTask implements Runnable, LongConsumer {
            private VertexIterator myIter;
            private int _partitionId;
            private EntityFieldAccessor[] _tmpVertexFields;

            SeedTask(int partitionId) {
                _partitionId = partitionId;
            }


            @Override
            public void run() {
                myIter = _rap.getNewAllVerticesIterator();
                _tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

                if (_infectedSet[_partitionId] != null) {
                    _infectedSet[_partitionId].forEach(this);
                }
            }


            @Override
            public void accept(long value) {
                seedVertex(value);
            }


            private void seedVertex(long vertexId) {
                myIter.reset(vertexId);
                if (myIter.isValid()) {
                    infectVertex(vertexId, myIter.getField(VFIELD_IS_HALT).getBoolean(), _initialInfectionTime, myIter.getOutgoingEdges());
                }
                else {
                    //System.out.println("Not seeding invalid vertex: " + vertexId);
                }
            }


            /* Infects a single vertex and it's destinations (created after the time of infection) */
            private void infectVertex(long vertexId, boolean isHalt, long infectionTime, EdgeIterator iter) {
                if (infect(0, isHalt, vertexId, infectionTime)) {
                    while (iter.hasNext()) {
                        iter.next();

                        if (iter.isDstVertexLocal() && iter.getCreationTime() >= infectionTime) {
                            long dstVertexId = iter.getDstVertexId();
                            boolean dstIsHalt = _avpm.getField(dstVertexId, VFIELD_IS_HALT, _tmpVertexFields).getBoolean();
                            infect(0, dstIsHalt, dstVertexId, iter.getCreationTime());
                        }
                        else {
                            // DO SOMETHING ELSE
                        }
                    }
                }
                else {
                    //System.out.println("NOT INFECTING CHILDREN OF: " + vertexId);;
                }
            }
        }


        private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);

        private StringBuilder getVertexName(long vertexId, EntityFieldAccessor[] efa) {
            StringBuilder name = _tmpSBTL.get();

            name.setLength(0);
            StringBuilder sb = _avpm.getField(vertexId, VFIELD_ADDR_CHAIN, efa).getString();
            if (sb!=null && sb.length()>0) {
                name.append(sb);
                return name;
            }

            sb = _avpm.getField(vertexId, VFIELD_TXN_HASH, efa).getString();
            if (sb!=null) {
                name.append(sb);
                return name;
            }

            return name;
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
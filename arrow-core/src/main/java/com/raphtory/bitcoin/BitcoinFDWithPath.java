/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample implementation of a forward-diffusion algorithm
 * maintaining the paths.
 */
public class BitcoinFDWithPath {
    public static void main(String[] args) throws Exception {
        if (args==null || args.length<3) {
            System.err.println("Usage: BitcoinFDWithPath ARROW_DST_DIR '2022-04-04 12:05:00' root]");
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
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long initialInfectionTime = LocalDateTime.parse(args[1], formatter).toEpochSecond(ZoneOffset.UTC);
        long root = Long.parseLong(args[2]);


        System.out.println(new Date() + ": STARTING");
        long then = System.currentTimeMillis();
        rap.getVertexMgr().loadFiles();
        rap.getEdgeMgr().loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");

        BitcoinFDWithPath fdp = new BitcoinFDWithPath(rap, 100000, root, initialInfectionTime, true);
        fdp.start();

        System.exit(0);
    }


    /**
     * This class is used to store a single path-entry
     */
    private static class PathEntry {
        public PathEntry(long root, long vertexId, long parentId, long time) {
            _root = root;
            _vertexId = vertexId;
            _parentId = parentId;
            _time = time;
        }


        long _root;
        long _vertexId;
        long _parentId;
        long _time;


        @Override
        public String toString() {
            return _root + ", " + _vertexId + ", " + _parentId + ", " + _time;
        }
    }


    private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);

    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;

    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;
    private final int VFIELD_IS_HALT;

    // TODO Remove root from PathEntry...the root is always _root;
    private final int _nIterations;
    private final ArrayList<Future<?>> _tasks = new ArrayList<>();
    private AtomicLong _nChanges = new AtomicLong(0);
    private final boolean _useHaltFlag;
    private long _rootNode;
    private long _initialInfectionTime;
    private RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;

    // These fields are accessed in a multi-threaded manner
    // so access to them needs to be controlled.
    private final ArrayList<PathEntry>[] _stepPaths;
    private final Long2BooleanOpenHashMap[] _stepStates;
    private final Long2LongOpenHashMap[] _stepTimes;
    private final LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];



    @SuppressWarnings("unchecked")
    public BitcoinFDWithPath(RaphtoryArrowPartition rap, int nIterations, long rootNode, long initialInfectionTime, boolean useHaltFlag) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();

        VFIELD_ADDRESS_CHAIN = rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = rap.getVertexFieldId("transaction_hash");
        VFIELD_IS_HALT = rap.getVertexFieldId("is_halt");

        _nIterations = nIterations;
        _rootNode = rootNode;
        _initialInfectionTime = initialInfectionTime;
        _useHaltFlag = useHaltFlag;

        int nPartitions = _avpm.nPartitions();
        System.out.println("NPARTITIONS=" + nPartitions);

        _stepStates = new Long2BooleanOpenHashMap[nPartitions];
        _stepPaths = new ArrayList[nPartitions];
        _stepTimes = new Long2LongOpenHashMap[nPartitions];

        _changes2[0] = new LongOpenHashSet[nPartitions];
        _changes2[1] = new LongOpenHashSet[nPartitions];

        for (int i=0; i<nPartitions; ++i) {
            _stepStates[i] = new Long2BooleanOpenHashMap();
            _stepPaths[i] = new ArrayList<>();
            _stepTimes[i] = new Long2LongOpenHashMap();
            _stepTimes[i].defaultReturnValue(-1L);

            _changes2[0][i] = new LongOpenHashSet();
            _changes2[1][i] = new LongOpenHashSet();
        }
    }


    /* Runs the forward-diffusion algorithm... */
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
            System.out.println("ITERATION: " + i + " , changes=" + _nChanges.get() + " took " + (now - prevNow) + "ms");
            prevNow = now;
        }

        now = System.currentTimeMillis();
        System.out.println("FINISHED: " + (now - then) + "ms");

        then = System.currentTimeMillis();
        dumpPath(_rootNode);
        dump();
        now = System.currentTimeMillis();
        System.out.println("MT2PFD SAVE OUTPUT: " + (now-then) + "ms");
    }


    private StringBuilder getVertexName(long vertexId, EntityFieldAccessor[] efa) {
        StringBuilder name = _tmpSBTL.get();

        name.setLength(0);
        StringBuilder sb = _avpm.getField(vertexId, VFIELD_ADDRESS_CHAIN, efa).getString();
        if (sb!=null && sb.length()>0) {
            name.append(sb);
            return name;
        }

        sb = _avpm.getField(vertexId, VFIELD_TX_HASH, efa).getString();
        if (sb!=null) {
            name.append(sb);
            return name;
        }

        return name;
    }


    /* Outputs the path entries to a file */
    private void dumpPath(long root) throws Exception {
        File dir = new File("testpfdoutput");
        if (!dir.exists()) {
            dir.mkdir();
        }

        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(dir, "paths.txt")), 1024 * 1024);

        os.write("root, vertexId, parentId, time\n".getBytes());

        for (int i=0; i<_stepPaths.length; ++i) {
            ArrayList<PathEntry> paths = _stepPaths[i];

            for (PathEntry pe : paths) {
                os.write(pe.toString().getBytes());
                os.write((byte) '\n');
            }
        }

        os.close();
    }


    /* Outputs the path entries to a series of files */
    private void dump() throws Exception {
        File dir = new File("testpfdoutput");
        if (!dir.exists()) {
            dir.mkdir();
        }

        ArrayList<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < _stepStates.length; ++i) {
            final int ii = i;
            if (_stepStates[i].size()!=0) {
                tasks.add(_pool.submitTask(() -> dumpRawFile(ii)));
            }
        }

        _pool.waitTilComplete(tasks);
        tasks.clear();
    }


    /* Outputs the path entries for a single arrow partition */
    private void dumpRawFile(int partId) {
        File outFile = new File("testpfdoutput", "PFD.output" + partId + ".csv");

        try {
            FileOutputStream fos = new FileOutputStream(outFile);
            BufferedOutputStream bw = new BufferedOutputStream(fos, 65536 * 4);
            byte[] BTRUE = ", true\n".getBytes();
            byte[] COMMA = ", ".getBytes();

            bw.write("name, vertex-id, infected\n".getBytes());

            Long2BooleanOpenHashMap map = _stepStates[partId];

            // Get somewhere to store the field values...
            EntityFieldAccessor[] tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

            map.keySet().forEach(l -> {
                StringBuilder name = getVertexName(l, tmpVertexFields);

                try {
                    outputBytes(name.toString().getBytes(), bw); // XXX GARBAGE
                    outputBytes(COMMA, bw);

                    outputNumber(l, bw);

                    outputBytes(BTRUE, bw);
                }
                catch (Exception e) {
                    System.out.println("Exception: " + e);
                    e.printStackTrace(System.out);
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


    /* Initial setup step */
    private void step() {
        long then = System.currentTimeMillis();

        seedVertex(_rootNode);

        long now = System.currentTimeMillis();
        System.out.println("PFD STEP: took: " + (now-then) +"ms");
    }


    /* Seeds the root vertex and performs the initial set of infections */
    private void seedVertex(long vertexId) {
        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        EntityFieldAccessor[] tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

        iter.reset(vertexId);

        if (iter.isValid()) {
            boolean isHalt = iter.getField(VFIELD_IS_HALT).getBoolean();
            infectVertex(vertexId, isHalt, _initialInfectionTime, iter.getOutgoingEdges(), tmpVertexFields);
        }
        else {
            System.out.println("Not seeding invalid vertex: " + vertexId);
        }
    }


    /* Infects a single vertex and it's destinations (created after the time of infection) */
    private void infectVertex(long vertexId, boolean isHalt, long infectionTime, EdgeIterator edgeIter, EntityFieldAccessor[] tmpVertexFields) {
        if (infect(0, isHalt, vertexId, infectionTime, vertexId, vertexId)) {
            while (edgeIter.hasNext()) {
                edgeIter.next();

                if (edgeIter.isDstVertexLocal() && edgeIter.getCreationTime() >= infectionTime) {
                    long dstVertexId = edgeIter.getDstVertexId();
                    boolean dstIsHalt = _avpm.getField(dstVertexId, VFIELD_IS_HALT, tmpVertexFields).getBoolean();
                    infect(0, dstIsHalt, dstVertexId, edgeIter.getCreationTime(), vertexId, vertexId);
                }
                else {
                    // Placeholder for remote vertices: DO SOMETHING ELSE
                }
            }
        }
        else {
            //System.out.println("NOT INFECTING CHILDREN OF: " + vertexId);;
        }
    }


    /**
     * This class is used to process each iteration of infection after
     * the initial seeding. One instance per arrow partition.
     * Each arrow partition is iterated over in its own thread.
     */
    private class IterateTask implements Runnable, LongConsumer {
        private VertexIterator _allVerticesIter;
        private int _iteration;
        private int _partitionId;
        private EntityFieldAccessor[] _tmpVertexFields;

        private IterateTask(int partitionId, int iteration) {
            _partitionId = partitionId;
            _iteration = iteration;
        }


        /* Iterates over the infected vertices in this partition */
        @Override
        public void run() {
            _allVerticesIter = _rap.getNewAllVerticesIterator();
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
            _allVerticesIter.reset(vertexId);

            if (!_useHaltFlag || !_allVerticesIter.getField(VFIELD_IS_HALT).getBoolean()) {
                EdgeIterator wves = _allVerticesIter.getOutgoingEdges();
                while (wves.hasNext()) {
                    wves.next();

                    if (wves.isDstVertexLocal()) {
                        if (wves.getCreationTime() >= infectionTime) {
                            long dstVertexId = wves.getDstVertexId();
                            boolean dstIsHalt = _avpm.getField(dstVertexId, VFIELD_IS_HALT, _tmpVertexFields).getBoolean();
                            infect(_iteration, dstIsHalt, dstVertexId, wves.getCreationTime(), _rootNode, vertexId);
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
    private boolean infect(int iteration, boolean isHalt, long vertexId, long infectionTime, long rootVertex, long parentId) {
        int partId = _avpm.getPartitionId(vertexId);
        Long2BooleanOpenHashMap state = _stepStates[partId];
        int changeId = iteration & 0x01;

        synchronized (state) {
            boolean prevInfected = state.get(vertexId);

            if (!_useHaltFlag && isHalt) {
                if (!prevInfected) {
                    // If halt, infect once only!
                    state.put(vertexId, true);
                    _stepTimes[partId].put(vertexId, infectionTime);
                    _changes2[changeId][partId].add(vertexId);
                    _nChanges.getAndIncrement();
                    //System.out.println("Infected halt vertex: " + getVertexName(vertexId));
                }

                addPathEntry(vertexId, infectionTime, rootVertex, parentId);

                //System.out.println("NOT Infecting halt vertex: " + getVertexName(vertexId));
                return false;
            }

            if (!prevInfected) {
                // First time this vertex has been infected
                state.put(vertexId, true);
                _stepTimes[partId].put(vertexId, infectionTime);
                _changes2[changeId][partId].add(vertexId);
                _nChanges.getAndIncrement();

                addPathEntry(vertexId, infectionTime, rootVertex, parentId);

                //System.out.println("Infecting vertex: " + getVertexName(vertexId) + " at " + new Date(infectionTime*1000L));
                return true;
            }
            else if (_stepTimes[partId].get(vertexId)>infectionTime) {
                // Only re-infect further edges if created after this infection
                _stepTimes[partId].put(vertexId, infectionTime);
                _changes2[changeId][partId].add(vertexId);
                _nChanges.getAndIncrement();

                addPathEntry(vertexId, infectionTime, rootVertex, parentId);

                //System.out.println("Reinfecting vertex: " + getVertexName(vertexId) + " at " + new Date(infectionTime*1000L));
                return true;
            }

            //System.out.println("Not infecting " + getVertexName(vertexId) + " as time is too low: infectionTime=" + new Date(infectionTime*1000L) + ", previousluInfected=" + new Date(_stepTimes[partId].get(vertexId)*1000L));
            return false;
        }
    }


    // Appends a new path-entry to this path
    private void addPathEntry(long vertexId, long infectionTime, long rootVertex, long parentId) {
        int partId = _avpm.getPartitionId(vertexId);
        _stepPaths[partId].add(new PathEntry(rootVertex, vertexId, parentId, infectionTime));
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
            _tasks.clear();

            int prevChanges = (iteration-1) & 0x1;

            // Process the change-set from the "last" iteration
            for (int i=0; i<nPartitions; ++i) {
                if (_changes2[prevChanges][i].size()!=0) {
                    _tasks.add(_pool.submitTask(new IterateTask(i, iteration)));
                }
            }
        }

        waitTilComplete();
        _tasks.clear();

        return _nChanges.get()!=0;
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
/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample implementation of a forward-diffusion algorithm
 * maintaining the paths.
 */
public class GPFDWithPath {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 2) {
            System.err.println("Usage: GPFDWithPath ARROW_DST_DIR root]");
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

        long root = Long.parseLong(args[1]);

        System.out.println(new Date() + ": STARTING");
        long then = System.currentTimeMillis();
        rap.getVertexMgr().loadFiles();
        rap.getEdgeMgr().loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": GP: ARROW LOADING TOOK: " + (now - then) + "ms");

        GPFDWithPath fdp = new GPFDWithPath(rap, 100000, root);
        fdp.start();

        System.exit(0);
    }


    /**
     * This class is used to store a single path-entry
     */
    private static class PathEntry {
        public PathEntry(long root, long vertexId, long parentId) {
            _root = root;
            _vertexId = vertexId;
            _parentId = parentId;
        }

        long _root;
        long _vertexId;
        long _parentId;


        @Override
        public String toString() {
            return _root + ", " + _vertexId + ", " + _parentId;
        }
    }


    private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);

    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;

    // TODO Remove root from PathEntry...the root is always _root;
    private final int _nIterations;
    private final ArrayList<Future<?>> _tasks = new ArrayList<>();
    private AtomicLong _nChanges = new AtomicLong(0);
    private long _rootNode;
    private RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;

    // These fields are accessed in a multi-threaded manner
    // so access to them needs to be controlled.
    private final ArrayList<PathEntry>[] _stepPaths;
    private final Long2BooleanOpenHashMap[] _stepStates;
    private final LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];
    private final int USERID_FIELD;


    @SuppressWarnings("unchecked")
    public GPFDWithPath(RaphtoryArrowPartition rap, int nIterations, long rootNode) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();

        _nIterations = nIterations;
        _rootNode = rootNode;

        USERID_FIELD = _rap.getVertexFieldId("userid");

        int nPartitions = _avpm.nPartitions();
        System.out.println("NPARTITIONS=" + nPartitions);

        _stepStates = new Long2BooleanOpenHashMap[nPartitions];
        _stepPaths = new ArrayList[nPartitions];

        _changes2[0] = new LongOpenHashSet[nPartitions];
        _changes2[1] = new LongOpenHashSet[nPartitions];

        for (int i = 0; i < nPartitions; ++i) {
            _stepStates[i] = new Long2BooleanOpenHashMap();
            _stepPaths[i] = new ArrayList<>();

            _changes2[0][i] = new LongOpenHashSet();
            _changes2[1][i] = new LongOpenHashSet();
        }
    }


    /* Runs the forward-diffusion algorithm... */
    public void start() throws Exception {
        long then = System.currentTimeMillis();

        step();

        long now = System.currentTimeMillis();
        System.out.println("STEP: " + _nChanges.get() + " took " + (now - then) + "ms");

        long prevNow = now;

        for (int i = 1; i <= _nIterations; ++i) {
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
        System.out.println("MT2PFD SAVE OUTPUT: " + (now - then) + "ms");
    }


    private StringBuilder getVertexName(long vertexId, EntityFieldAccessor[] efa) {
        StringBuilder name = _tmpSBTL.get();

        name.setLength(0);
        StringBuilder sb = _avpm.getField(vertexId, USERID_FIELD, efa).getString();
        name.append(sb);
        return name;
    }


    /* Outputs the path entries to a file */
    private void dumpPath(long root) throws Exception {
        File dir = new File("testpfdoutput");
        if (!dir.exists()) {
            dir.mkdir();
        }

        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(dir, "paths.txt")), 1024 * 1024);

        os.write("root, vertexId, parentIdn".getBytes());

        for (int i = 0; i < _stepPaths.length; ++i) {
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
            if (_stepStates[i].size() != 0) {
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
        System.out.println("PFD STEP: took: " + (now - then) + "ms");
    }


    /* Seeds the root vertex and performs the initial set of infections */
    private void seedVertex(long vertexId) {
        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        EntityFieldAccessor[] tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

        iter.reset(vertexId);

        if (iter.isValid() && iter.getNOutgoingEdges() > 0 && iter.getNIncomingEdges() > 0) {
            infectVertex(vertexId, iter.getOutgoingEdges(), tmpVertexFields);
        }
        else {
            System.out.println("Not seeding invalid vertex: " + vertexId);
        }
    }


    /* Infects a single vertex and it's destinations (created after the time of infection) */
    private void infectVertex(long vertexId, EdgeIterator edgeIter, EntityFieldAccessor[] tmpVertexFields) {
        if (infect(0, vertexId, vertexId, vertexId)) {
            while (edgeIter.hasNext()) {
                edgeIter.next();

                infect(0, edgeIter.getDstVertexId(), vertexId, vertexId);
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
        private VertexIterator _raIterator;
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
            _raIterator = _rap.getNewAllVerticesIterator();
            _tmpVertexFields = _rap.getTmpVertexEntityFieldAccessors();

            // Iterate over the change-list from the PREVIOUS iteration
            int changeIndex = (_iteration - 1) & 0x1;
            _changes2[changeIndex][_partitionId].forEach(this);
        }


        @Override
        public void accept(long value) {
            processVertex(value);
        }


        /* Processes a single vertex */
        private void processVertex(long vertexId) {
            Long2BooleanOpenHashMap state = _stepStates[_partitionId];

            // Move to this vertex...
            _allVerticesIter.reset(vertexId);

            EdgeIterator wves = _allVerticesIter.getOutgoingEdges();
            while (wves.hasNext()) {
                wves.next();

                if (wves.isDstVertexLocal()) {
                    long dstVertexId = wves.getDstVertexId();

                    _raIterator.reset(dstVertexId);

                    if (_raIterator.isValid() && _raIterator.getNIncomingEdges() > 0 && _raIterator.getNOutgoingEdges() > 0 && !alreadySeen(dstVertexId, _rootNode, vertexId)) {
                        if (dstVertexId != _rootNode) {
                            infect(_iteration, dstVertexId, _rootNode, vertexId);
                        }
                    }
                }
                else {
                    // Placeholder for remote vertices: DO SOMETHING ELSE
                }
            }
        }
    }

    // Main infection algorithm
    private boolean infect(int iteration, long vertexId, long rootVertex, long parentId) {
        int partId = _avpm.getPartitionId(vertexId);
        Long2BooleanOpenHashMap state = _stepStates[partId];
        int changeId = iteration & 0x01;

        synchronized (state) {
            boolean prevInfected = state.get(vertexId);

            if (!prevInfected) {
                // First time this vertex has been infected
                state.put(vertexId, true);
                _changes2[changeId][partId].add(vertexId);
                _nChanges.getAndIncrement();

                addPathEntry(vertexId, rootVertex, parentId);

                //System.out.println("Infecting vertex: " + getVertexName(vertexId) + " at " + new Date(infectionTime*1000L));
                return true;
            }

            return false;
        }
    }


    // Appends a new path-entry to this path
    private void addPathEntry(long vertexId, long rootVertex, long parentId) {
        int partId = _avpm.getPartitionId(vertexId);
        _stepPaths[partId].add(new PathEntry(rootVertex, vertexId, parentId));
    }


    private boolean alreadySeen(long vertexId, long rootVertex, long parentId) {
        if (vertexId == rootVertex) {
            // Always accept the cycle
            return false;
        }

        int partId = _avpm.getPartitionId(vertexId);
        ArrayList<PathEntry> paths = _stepPaths[partId];
        if (paths == null) {
            return false;
        }

        // Check if this vertex has already been seen on this path
        int n = paths.size();
        for (int i=0; i<n; ++i) {
            PathEntry pe = paths.get(i);
            if (pe._root==rootVertex && (pe._parentId == vertexId || pe._vertexId == vertexId)) {
                return true;
            }
        }

        return false;
    }


    // Performs an iteration of infection.
    // Fires off 1 task per arrow partition
    private boolean iterate(int iteration) {
        _tasks.clear();
        int nPartitions = _avpm.nPartitions();

        synchronized (_tasks) {
            // Clear the change-set to be used for the "next" iteration
            final int nextChanges = iteration & 0x1;
            for (int i = 0; i < nPartitions; ++i) {
                final int ii = i;
                _tasks.add(_pool.submitTask(() -> _changes2[nextChanges][ii].clear()));
            }
            waitTilComplete();
            _tasks.clear();

            int prevChanges = (iteration - 1) & 0x1;

            // Process the change-set from the "last" iteration
            for (int i = 0; i < nPartitions; ++i) {
                if (_changes2[prevChanges][i].size() != 0) {
                    _tasks.add(_pool.submitTask(new IterateTask(i, iteration)));
                }
            }
        }

        waitTilComplete();
        _tasks.clear();

        return _nChanges.get() != 0;
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
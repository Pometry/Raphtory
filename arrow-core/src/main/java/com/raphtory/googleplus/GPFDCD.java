/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class GPFDCD {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: GPFDCD ARROW_DST_DIR]");
            System.err.println("Note, the dst dir should contain the edge and vertex arrow files!");
            System.exit(1);
        }

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new GPSchema();
        cfg._arrowDir = args[0];
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

        GPFDCD fdp = new GPFDCD(rap, 6);
        fdp.start();

        System.exit(0);
    }


    private static class Message {
        public Message(long root, long sender) {
            _root = root;
            _sender = sender;
        }

        long _root;
        long _sender;


        @Override
        public String toString() {
            return _root + ", " + _sender;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(_root) | Long.hashCode(_sender);
        }


        @Override
        public boolean equals(Object o) {
            Message m = (Message)o;

            return _root==m._root && _sender==m._sender;
        }
    }


    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;

    private final int _nIterations;
    private final ArrayList<Future<?>> _tasks = new ArrayList<>();
    private final AtomicLong _nChanges = new AtomicLong(0);
    private final RaphtoryThreadPool _pool = RaphtoryThreadPool.THREAD_POOL;

    // These fields are accessed in a multi-threaded manner
    // so access to them needs to be controlled.
    private final Long2BooleanOpenHashMap[] _stepStates;

    @SuppressWarnings("unchecked")
    private final Long2ObjectOpenHashMap<ObjectOpenHashSet<Message>>[][] _messages = new Long2ObjectOpenHashMap[2][];
    private final LongOpenHashSet[][] _changes2 = new LongOpenHashSet[2][];


    @SuppressWarnings("unchecked")
    public GPFDCD(RaphtoryArrowPartition rap, int nIterations) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();

        _nIterations = nIterations;

        int nPartitions = _avpm.nPartitions();
        System.out.println("NPARTITIONS=" + nPartitions);

        _stepStates = new Long2BooleanOpenHashMap[nPartitions];

        _messages[0] = new Long2ObjectOpenHashMap[nPartitions];
        _messages[1] = new Long2ObjectOpenHashMap[nPartitions];

        _changes2[0] = new LongOpenHashSet[nPartitions];
        _changes2[1] = new LongOpenHashSet[nPartitions];

        for (int i=0; i<nPartitions; ++i) {
            _stepStates[i] = new Long2BooleanOpenHashMap();

            _changes2[0][i] = new LongOpenHashSet();
            _changes2[1][i] = new LongOpenHashSet();

            _messages[0][i] = new Long2ObjectOpenHashMap<>();
            _messages[1][i] = new Long2ObjectOpenHashMap<>();
        }
    }


    /* Runs the forward-diffusion algorithm... */
    public void start() throws Exception {
        long then = System.currentTimeMillis();

        step();

        long now = System.currentTimeMillis();
        System.out.println("STEP: " + _nChanges.get() + " took " + (now - then) + "ms");

        long prevNow = now;

        for (int i=1; i<_nIterations; ++i) {
            _nChanges.set(0);
            boolean finished = !iterate(i);

            now = System.currentTimeMillis();
            System.out.println("ITERATION: " + i + ", changes=" + _nChanges.get() + " took " + (now - prevNow) + "ms");
            prevNow = now;

            if (finished) {
                break;
            }
        }

        now = System.currentTimeMillis();
        System.out.println("FINISHED: " + (now - then) + "ms");
    }


    public void waitTilComplete() {
        _pool.waitTilComplete(_tasks);
    }


    /* Initial setup step */
    private void step() {
        long then = System.currentTimeMillis();

        VertexIterator.MTAllVerticesManager mgr = _rap.getNewMTAllVerticesManager(_pool);
        mgr.start(this::seedPartition);
        mgr.waitTilComplete();

        dumpState("STEP:", 0);

        long now = System.currentTimeMillis();
        System.out.println("PFD STEP: took: " + (now - then) + "ms");
    }


    /* Seeds the root vertex and performs the initial set of infections */
    private void seedPartition(int partitionId, VertexIterator iterator) {
        VertexIterator childIterator = _rap.getNewAllVerticesIterator();

        Message m;

        while (iterator.hasNext()) {
            m = null;
            long myId = iterator.next();

            if (iterator.getNOutgoingEdges()>0 && iterator.getNIncomingEdges()>0) {
                EdgeIterator edgeIter = iterator.getOutgoingEdges();
                while (edgeIter.hasNext()) {
                    edgeIter.next();

                    long dstVertexId = edgeIter.getDstVertexId();
                    if (true || myId != dstVertexId) {
                        childIterator.reset(dstVertexId);
                        if (childIterator.getNOutgoingEdges() > 0) {
                            if (m == null) {
                                m = new Message(myId, myId);
                            }

                            sendMessage(0, dstVertexId, m);
                        }
                    }
                }
            }

            //System.out.println("STEP FOR: " + myId);
            //dumpState(0);
            //System.out.println("\n\n");
        }
    }


    private void sendMessage(int iteration, long vertexId, Message m) {
        int partId = _avpm.getPartitionId(vertexId);
        Long2BooleanOpenHashMap state = _stepStates[partId];
        int changeId = iteration & 0x01;

        synchronized (state) {
            state.put(vertexId, true);
            _changes2[changeId][partId].add(vertexId);
            _nChanges.getAndIncrement();

            Long2ObjectOpenHashMap<ObjectOpenHashSet<Message>> map = _messages[changeId][partId];
            ObjectOpenHashSet<Message> set = map.get(vertexId);
            if (set==null) {
                set = new ObjectOpenHashSet<>();
                map.put(vertexId, set);
            }

            set.add(m);
        }

        _nChanges.incrementAndGet();
    }


    private void dumpState(String title, int changeId) {
        System.out.println(title);

        Long2ObjectOpenHashMap<ObjectOpenHashSet<Message>> map = _messages[changeId][0];
        for (int i=0; i<15; ++i) {
            ObjectOpenHashSet<Message> set = map.get(i);
            if (set==null) {
                continue;
            }

            System.out.println("Vertex: " + i + " --> " + set.toString());
        }

        System.out.println("Changes: " + _stepStates[0]);

        System.out.println("\n");
    }

    private void sendMessage(int iteration, long vertexId, ObjectOpenHashSet<Message> msgs) {
        int partId = _avpm.getPartitionId(vertexId);
        Long2BooleanOpenHashMap state = _stepStates[partId];
        int changeId = iteration & 0x01;

        synchronized (state) {
            state.put(vertexId, true);
            _changes2[changeId][partId].add(vertexId);
            _nChanges.getAndIncrement();

            Long2ObjectOpenHashMap<ObjectOpenHashSet<Message>> map = _messages[changeId][partId];
            ObjectOpenHashSet<Message> set = map.get(vertexId);
            if (set==null) {
                set = new ObjectOpenHashSet<>();
                map.put(vertexId, set);
            }

            // One of these must be faster/less garbage:
            set.addAll(msgs);
            //final ObjectOpenHashSet<Message> set2 = set;
            //msgs.forEach(m -> set2.add(m));
            _nChanges.incrementAndGet();
            // Can make this function and the lambda, members of IterateTask to reduce GC
        }
    }



    private class IterateTask implements Runnable {
        private VertexIterator _allVerticesIter;
        private VertexIterator _childIterator;
        private final int _iteration;
        private final int _partitionId;
        private final int _prevChangeIndex;
        private final MutableBoolean _isCyclic = new MutableBoolean(false);
        private final Long2ObjectOpenHashMap<ObjectOpenHashSet<Message>> _prevMessageMap;

        private IterateTask(int partitionId, int iteration) {
            _partitionId = partitionId;
            _iteration = iteration;
            _prevChangeIndex = (_iteration - 1) & 0x1;
            _prevMessageMap = _messages[_prevChangeIndex][_partitionId];
        }


        /* Iterates over the infected vertices in this partition */
        @Override
        public void run() {
            _allVerticesIter = _rap.getNewAllVerticesIterator();
            _childIterator = _rap.getNewAllVerticesIterator();


            // Iterate over the change-list from the PREVIOUS iteration
            _changes2[_prevChangeIndex][_partitionId].forEach(this::processVertex);
        }


        /* Processes a single vertex */
        private void processVertex(long vertexId) {
            ObjectOpenHashSet<Message> messageSet = _prevMessageMap.get(vertexId);

            if (messageSet!=null && !messageSet.isEmpty()) {
                if (_iteration == _nIterations-1) {
                    _isCyclic.setFalse();

                    for (Message m: messageSet) {
                        if (m._root == vertexId) {
                            _isCyclic.setTrue();
                            break;
                        }
                    }

                    if (_isCyclic.isTrue()) {
                        // DUMP THE DATA
                        // XXX FIXME!
                        System.out.println("CYCLE FOR: " + vertexId);
                    }
                }
                else {
                    messageSet.removeIf(m -> m._root==vertexId);

                    // Move to this vertex...
                    _allVerticesIter.reset(vertexId);

                    EdgeIterator outgoingEdges = _allVerticesIter.getOutgoingEdges();
                    while (outgoingEdges.hasNext()) {
                        outgoingEdges.next();

                        long dstVertexId = outgoingEdges.getDstVertexId();
                        if (true || dstVertexId!=vertexId) { // Ignore simple back-links
                            _childIterator.reset(dstVertexId);

                            if (_childIterator.getNOutgoingEdges()>0) {
                                sendMessage(_iteration, dstVertexId, messageSet);
                            }
                        }
                    }
                }
            }
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
                _tasks.add(_pool.submitTask(() -> {
                    _stepStates[ii].clear();
                    _changes2[nextChanges][ii].clear();
                    _messages[nextChanges][ii].values().forEach(a -> a.clear());
                }));
            }
            waitTilComplete();
            _tasks.clear();

            int prevChanges = (iteration - 1) & 0x1;

            // Process the change-set from the "last" iteration
            for (int i=0; i<nPartitions; ++i) {
                if (_changes2[prevChanges][i].size() != 0) {
                    _tasks.add(_pool.submitTask(new IterateTask(i, iteration)));
                }
            }
        }

        waitTilComplete();
        _tasks.clear();

        dumpState("ITERATION: " + iteration, iteration & 0x1);


        return _nChanges.get() != 0;
    }
}

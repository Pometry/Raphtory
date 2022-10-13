/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Edge;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.concurrent.Future;

/**
 * This class provides iterators that can iterate over edges in a relatively efficient manner.
 *<p>
 * Single-threaded and multi-threaded iterators are provided as well as some support for
 * random-access of edges.
 *<p>
 * <p>TODO: Check the use of thread-locals here. Some use is made of them to reduce memory consumption
 * <p>TODO: during heavy iteration. However, there may be better ways to do this - and it's quite likely
 * <p>TODO: that the memory consumption is pretty minor anyway.
 *<p>
 * Base class for edge iterators.
 * <p>
 * It encapsulates common functions such as getting fields and properties etc.
 * Some of the methods within this class return fields, properties and further iterators.
 * <p>
 * It should be noted that all of those objects will be re-used (or become invalid) if
 * the iterator moves to another vertex - meaning that you should use them immediately
 * and copy the value, if required.
 */
public abstract class EdgeIterator {
    private static final ThreadLocal<WindowedEdgeIterator> _vwesTL = ThreadLocal.withInitial(WindowedEdgeIterator::new);
    private static final ThreadLocal<AllEdgesIterator> _aewesTL = ThreadLocal.withInitial(AllEdgesIterator::new);

    /**
     * MTEdgeIteratorConsumer - clients of the multi-thread edge iterators will
     * need to implement this interface to receive thread-specific iterators.
     *
     * @see MTAllEdgesManager
     */
    public interface MTEdgeIteratorConsumer {
        public void accept(int partitionId, EdgeIterator iter);
    }

    private boolean _hasNext = false;
    private boolean _getNext = true;

    protected EdgePartitionManager _aepm;
    protected EdgePartition _edgePartition;
    protected int _partitionId = -1;
    protected int _edgeRowId;
    protected long _edgeId;
    protected EntityFieldAccessor[] _efa = null;
    protected VersionedEntityPropertyAccessor[] _vefa = null;
    protected ArrowPropertyIterator[] _propertyIterators = null;
    protected VersionedEntityPropertyAccessor[] _propertyIteratorAccessors = null;


    /**
     * Initialise this iterator
     *
     * @param aepm the edge partition manager to use
     */
    protected void init(EdgePartitionManager aepm) {
        _aepm = aepm;
        _edgePartition = null;
        _partitionId = -1;
        _edgeRowId = -1;
        _edgeId = -1L;
        _hasNext = false;
        _getNext = true;
    }


    /**
     * Initialise this iterator
     *
     * @param aepm the edge partition manager to use
     * @param p the partition we're iterating over
     */
    protected void init(EdgePartitionManager aepm, EdgePartition p) {
        _aepm = aepm;
        _edgePartition = p;
        _partitionId = _edgePartition.getPartitionId();
        _edgeRowId = -1;
        _edgeId = -1L;
        _hasNext = false;
        _getNext = true;
    }


    /**
     * Resets this iterator - repositioning it at the specified edge.
     *
     * @param edgeId the edge-id to move to
     */
    public void reset(long edgeId) {
        _edgeId = edgeId;
        _edgeRowId = _aepm.getRowId(edgeId);
        _edgePartition = _aepm.getPartition(_aepm.getPartitionId(edgeId));
        _hasNext = _edgeId!=-1L && _edgePartition!=null;
        _getNext = false;
    }


    /**
     * Retrieves a field within the current edge directly
     *
     * @param field the field in question
     *
     * @return the field accessor that contains the value
     */
    public EntityFieldAccessor getField(int field) {
        if (_efa==null) {
            _efa = _aepm._raphtoryPartition.createEdgeEntityFieldAccessors();
        }

        _edgePartition._fieldAccessors[field].load(_efa[field], _edgeRowId);
        return _efa[field];
    }


    /**
     * Retrieves a property within the current edge directly
     *
     * @param field the property in question
     *
     * @return the property accessor that contains the value
     */
    public VersionedEntityPropertyAccessor getProperty(int field) {
        if (_vefa==null) {
            _vefa = _aepm._raphtoryPartition.createEdgeEntityPropertyAccessors();
        }

        int row = _edgePartition.getPropertyPrevPtrByRow(field, _edgeRowId);
        if (row==-1) {
            return null;
        }

        _edgePartition._propertyStores[field].retrieveProperty(row, _vefa[field]);
        return _vefa[field];
    }


    /**
     * @return the next edge-id, or -1L if unavailable
     */
    public final long next() {
        if (_getNext) {
            moveToNext();
        }

        _getNext = true;

        return _edgeId;
    }


    /**
     * @return true if there are further edges within this iterator, false otherwise
     */
    public final boolean hasNext() {
        if (_getNext) {
            _hasNext = moveToNext();
            _getNext = false;
        }

        return _hasNext;
    }


    /**
     * @return the partition associated with the current edge
     */
    public final EdgePartition getPartition() {
        return _edgePartition;
    }


    /**
     * Moves to the next matching edge in this iteration
     *
     * @return true iff there's a matching edge, false otherwise
     */
    protected abstract boolean moveToNext();


    /**
     * Retrieves the current edge as a POJO Edge. The ref-count will be 1.
     *
     * @return the POJO Edge
     */
    public Edge getEdge() {
        return _edgePartition.getEdge(_edgeId);
    }


    /**
     * @return the src-vertex-id for the current edge
     */
    public long getSrcVertexId() {
        return _edgePartition.getSrcVertexId(_edgeRowId);
    }


    /**
     * @return the dst-vertex-id for the current edge
     */
    public long getDstVertexId() {
        return _edgePartition.getDstVertexId(_edgeRowId);
    }


    /**
     * @return true if the src-vertex is local, false otherwise
     */
    public boolean isSrcVertexLocal() {
        return _edgePartition.isSrcVertexLocal(_edgeRowId);
    }


    /**
     * @return true if the dst-vertex is local, false otherwise
     */
    public boolean isDstVertexLocal() {
        return _edgePartition.isDstVertexLocal(_edgeRowId);
    }


    /**
     * @return the creation time for the current edge
     */
    public long getCreationTime() { return _edgePartition._getCreationTime(_edgeRowId); }


    /**
     * Returns a property iterator for the specified property for the current edge.
     * The returned iterator should be used immediately as it will be re-used if the
     * edge iterator moves to another edge.
     *
     * @param property the property in question
     *
     * @return the property-iterator
     */
    public ArrowPropertyIterator getPropertyHistory(int property) {
        if (_propertyIterators==null) {
            _propertyIteratorAccessors = _edgePartition._aepm._raphtoryPartition.createEdgeEntityPropertyAccessors();
            _propertyIterators = new ArrowPropertyIterator[_propertyIteratorAccessors.length];
            for (int i=0; i<_propertyIterators.length; ++i) {
                _propertyIterators[i] = new ArrowPropertyIterator.AllPropertiesIterator();
                _propertyIterators[i].init(_edgePartition._propertyStores[property]._store, _edgePartition._propertyStores[property]._accessor, _propertyIteratorAccessors[i], -1);
            }
        }

        _propertyIterators[property].init(_edgePartition._propertyStores[property]._store, _edgePartition._propertyStores[property]._accessor, _propertyIteratorAccessors[property], _edgePartition.getPropertyPrevPtrByRow(property, _edgeRowId));

        return _propertyIterators[property];
    }


    /**
     * Multi-threaded edge iterator manager
     *
     * This class allows the iteration of vertices processing each edge partition in a separate thread.
     */
    public static class MTAllEdgesManager {
        private final ArrayList<Future<?>> _tasks = new ArrayList<>();
        EdgePartitionManager _aepm;
        RaphtoryThreadPool _pool;

        protected MTAllEdgesManager() {}


        /**
         * Initialise this instance
         *
         * @param aepm the edge partition manager to use
         * @param pool the thread pool to use
         */
        protected void init(EdgePartitionManager aepm, RaphtoryThreadPool pool) {
            _aepm = aepm;
            _pool = pool;
        }


        /**
         * Start the iteration - invoking the consumer in multiple threads
         *
         * @param consumer the consumer to invoke
         */
        public synchronized void start(MTEdgeIteratorConsumer consumer) {
            int partitionId = 0;
            _tasks.clear();

            for (;;) {
                EdgePartition p = _aepm.getPartition(partitionId++);
                if (p == null) {
                    break;
                }

                IterTask task = new IterTask(p, consumer);
                Future<?> f = _pool.submitTask(task);
                _tasks.add(f);
            }
        }


        /**
         * Forces the caller to wait until the sub-tasks have completed.
         */
        public synchronized void waitTilComplete() {
            _pool.waitTilComplete(_tasks);
        }


        /**
         * Attempts to cancel the iteration process
         */
        public synchronized void cancel() {
            _pool.cancel(_tasks);
        }


        /**
         * Internal iteration task class - invoked once per partition in a separate thread
         */
        private class IterTask implements Runnable {
            protected final EdgePartition _pp;
            protected final MTEdgeIteratorConsumer _consumer;


            /**
             * Instantiate the IterTask
             *
             * @param p the vertex partition to iterate over
             * @param consumer the consumer to invoke
             */
            private IterTask(EdgePartition p, MTEdgeIteratorConsumer consumer) {
                _pp = p;
                _consumer = consumer;
            }


            /**
             * Task run method - gets hold of a AllVerticesIterator and invokes the consumer,
             * allowing it to iterate over the vertices in it's own thread
             */
            @Override
            public void run() {
                AllEdgesIterator iter = getAllEdges(_aepm, _pp);
                _consumer.accept(_pp.getPartitionId(), iter);
            }
        }
    }


    /**
     * This class provides a single-threaded iterator iterating over a set of edges
     * that are active within a time window.
     *<p>
     * <p>TODO: Build a multi-threaded implementation?
     */
    public static class WindowedEdgeIterator extends EdgeIterator {
        protected LongOpenHashSet _processedEdges = new LongOpenHashSet(1024);
        protected long _minTime;
        protected long _maxTime;

        protected int _firstIndex;
        protected int _lastIndex;
        protected int _index;
        protected boolean _firstTime = true;

        private WindowedEdgeIterator() {}


        /**
         * Initialises this instance.
         *
         * @param aepm the edge partition manager to use
         * @param minTime the start time of the window (inclusive)
         * @param maxTime the end time of the window (inclusive)
         */
        protected WindowedEdgeIterator(EdgePartitionManager aepm, long minTime, long maxTime) {
            init(aepm, minTime, maxTime);
        }


        /**
         * Initialises this instance.
         *
         * @param aepm the edge partition manager to use
         * @param minTime the start time of the window (inclusive)
         * @param maxTime the end time of the window (inclusive)
         */
        private void init(EdgePartitionManager aepm, long minTime, long maxTime) {
            super.init(aepm);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;
            _index = -1;
        }


        /**
         * Searches for the next edge that matches the time window.
         *
         * @return the next matching edge-id
         */
        private long getNext() {
            for (;;) {
                if (_edgePartition == null) {
                    _edgePartition = _aepm.getPartition(++_partitionId);
                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _processedEdges.clear();
                    _edgeRowId = -1;
                    _edgeId = -1L;
                }

                if (_edgePartition == null) {
                    _edgeRowId = -1;
                    _edgeId = -1L;
                    return -1L;
                }

                if (_lastIndex!=-1) {
                    if (_index >= _firstIndex) {
                        //System.out.println("RET: " + _index);
                        int historyRowId = _edgePartition._history.getHistoryRowIdBySortedIndex(_index);
                        boolean alive = _edgePartition._history.getIsAliveByRowId(historyRowId);
                        _edgeRowId = _edgePartition._history.getEdgeLocalRowIdByHistoryRowId(historyRowId);

                        //System.out.println("LOOKING AT: " + _vertexRowId + " -> " + _p._getLocalVertexIdByRow(_vertexRowId));

                        if (!_processedEdges.contains(_edgeRowId)) {
                            _processedEdges.add(_edgeRowId);
                            if (alive) {
                                return _edgePartition._getLocalEdgeIdByRow(_edgeRowId);
                            }
                        }
                        else {
                            --_index;
                            continue;
                        }
                    }

                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _edgePartition = null;
                    _edgeRowId = -1;
                    _edgeId = -1L;
                }
                else {
                    _edgePartition._history.isAliveAtWithWindowVector(this);
                    if (_lastIndex==-1) {
                        _edgePartition = null;
                        _edgeRowId = -1;
                        _edgeId = -1L;
                    }
                    else {
                        _index = _lastIndex;
                    }
                    //System.out.println("WINDOW: " + _lastIndex + " -> " + _firstIndex);
                }
            }
        }


        /**
         * @return true if there are further edges within this iterator, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (!_firstTime) {
                --_index;
            }
            else {
                _firstTime = false;
            }

            _edgeId = getNext();
            return _edgeId !=-1L;
        }
    }


    /**
     * Retrieves all edges that fit within a time window.
     * Uses a thread-local to reduce memory consumption.
     *<p>
     * Currently unused.
     *
     * @param aepm the edge partition manager to use
     * @param minTime the start time of the time window
     * @param maxTime the end time of the time window
     *
     * @return a thread-local edge iterator, correctly initialised for a change
     */
    protected static WindowedEdgeIterator getNextAliveAtWithWindowVector(EdgePartitionManager aepm, long minTime, long maxTime) {
        WindowedEdgeIterator wes = _vwesTL.get();
        wes.init(aepm, minTime, maxTime);
        return wes;
    }


    /**
     * Returns a thread-local all-edges-iterator
     *
     * @param aepm the edge partition manager to use
     * @param p the edge partition to iterate over
     *
     * @return the initialised, thread-local iterator
     */
    protected static AllEdgesIterator getAllEdges(EdgePartitionManager aepm, EdgePartition p) {
        AllEdgesIterator aewes = _aewesTL.get();
        aewes.init(aepm, p);
        return aewes;
    }


    /**
     * Iterator that iterates over all vertices
     */
    public static class AllEdgesIterator extends EdgeIterator {
        protected long _index = -1L;
        protected boolean _doAll = true;


        private AllEdgesIterator() {}


        /**
         * Instantiates this iterator.
         *
         * @param aepm the edge partition manager to use
         */
        protected AllEdgesIterator(EdgePartitionManager aepm) {
            init(aepm);
        }


        /**
         * Resets this iterator - repositioning it at the specified edge.
         *
         * @param edgeId the edge-id to move to
         */
        @Override
        public void reset(long edgeId) {
            super.reset(edgeId);
            _partitionId = _edgePartition!=null ? _edgePartition._partitionId : -1;
        }


        /**
         * Initialises this instance
         *
         * @param aepm the edge partition manager to use
         */
        @Override
        protected void init(EdgePartitionManager aepm) {
            super.init(aepm);

            _index = -1L;
            _doAll = true;
        }


        /**
         * Initialises this instance
         *
         * @param aepm the edge partition manager to use
         * @param p the single partition we're iterating over
         */
        @Override
        protected void init(EdgePartitionManager aepm, EdgePartition p) {
            super.init(aepm, p);

            _index = -1L;
            _doAll = false;
        }


        /**
         * @return true if there are further edges within this iterator, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            for (;;) {
                if (_doAll && _edgePartition ==null) {
                    _edgePartition = _aepm.getPartition(++_partitionId);
                    _edgeRowId = -1;
                    _edgeRowId = -1;
                    _edgeId = -1L;
                }
                if (_edgePartition == null) {
                    _edgeRowId = -1;
                    _edgeId = -1L;
                    return false;
                }

                if (!_edgePartition.isValidRow(++_edgeRowId)) {
                    _edgePartition = null;
                    _edgeRowId = -1;
                    _edgeId = -1L;
                    continue;
                }

                _edgeId = _edgePartition._getLocalEdgeIdByRow(_edgeRowId);
                return true;
            }
        }
    }


    /**
     * This iterator works in conjunction with AllVerticesIterator and allows
     * the edges associated with a particular vertex to be iterated over.
     * Both incoming and outgoing edges will be iterated over.
     *
     * @see VertexIterator.AllVerticesIterator
     */
    public static class AllEdgesIteratorFromVertex extends EdgeIterator {
        private final EdgeIteratorFromVertex _incoming = new EdgeIteratorFromVertex();
        private final EdgeIteratorFromVertex _outgoing = new EdgeIteratorFromVertex();
        private EdgeIteratorFromVertex _delegate = null;


        /**
         * Initialises this instance
         *
         * @param aepm the edge partition to use
         * @param incomingEdgeId the head of the list of incoming edge-ids
         * @param outgoingEdgeId the head of the list of outgoing edge-ids
         */
        protected void init(EdgePartitionManager aepm, long incomingEdgeId, long outgoingEdgeId) {
            super.init(aepm);
            _incoming.init(aepm, incomingEdgeId, true);
            _outgoing.init(aepm, outgoingEdgeId, false);
            _delegate = _incoming;
        }


        /**
         * @return true if there are further edges within this iterator, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            boolean retval = _delegate.hasNext();
            if (!retval) {
                if (_delegate == _incoming) {
                    _delegate = _outgoing;
                    retval = _delegate.hasNext();
                }
            }

            if (retval) {
                _delegate.next();
                _edgePartition = _delegate._edgePartition;
                _partitionId = _delegate._partitionId;
                _edgeRowId = _delegate._edgeRowId;
                _edgeId = _delegate._edgeId;
            }

            return retval;
        }
    }

    /**
     * This iterator works in conjunction with AllVerticesIterator and allows
     * the edges associated with a particular vertex to be iterated over.
     * Either incoming or outgoing edges only will be iterated over.
     *
     * @see VertexIterator.AllVerticesIterator
     */
    public static class EdgeIteratorFromVertex extends EdgeIterator {
        private boolean _incoming;

        /**
         * Initialises this instance
         *
         * @param aepm the edge partition to use
         * @param edgeId the head of the list of edge list to iterate over
         * @param incoming true if this is the incoming list, false for the outgoing list
         */
        protected void init(EdgePartitionManager aepm, long edgeId, boolean incoming) {
            super.init(aepm);

            _incoming = incoming;

            if (edgeId!=-1L) {
                reset(edgeId);
            }
        }


        /**
         * @return true if there are further edges within this iterator, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (_edgePartition == null) {
                _edgeRowId = -1;
                _edgeId = -1L;
                return false;
            }

            if (_incoming) {
                _edgeId = _edgePartition._getPrevIncomingPtrByRow(_edgeRowId);
            }
            else {
                _edgeId = _edgePartition._getPrevOutgoingPtrByRow(_edgeRowId);
            }

            if (_edgeId==-1L) {
                _edgeRowId = -1;
                _edgePartition = null;
                _edgeRowId = -1;
                _edgeId = -1L;
                return false;
            }
            else {
                _edgeRowId = _aepm.getRowId(_edgeId);
                _edgePartition = _aepm.getPartition(_aepm.getPartitionId(_edgeId));
                return true;
            }
        }
    }


    /**
     * Iterator used for searching for edges with a know source and
     * destination vertex-id.
     */
    public static class MatchingEdgesIterator extends EdgeIterator {
        protected VertexPartition _vp;
        protected long _srcVertexId;
        protected long _dstVertexId;
        protected boolean _dstIsGlobal;
        protected int _nOutgoingEdges;
        protected VertexEdgeIndexPartition _veip;

        protected int _firstIndex;
        protected int _lastIndex;
        protected int _vertexRowId;
        protected int _index;


        /**
         * Initialises the iterator
         *
         * @param vp the vertex partition that the src-vertex is within
         * @param srcVertexId the src-vertex id
         * @param dstVertexId the dst-vertex id to use for searching
         * @param dstIsGlobal true
         * @param nOutgoingEdges the number of outgoing edges for the src-vertex
         */
        protected void findEdgesViaVertex(VertexPartition vp, long srcVertexId, long dstVertexId, boolean dstIsGlobal, int nOutgoingEdges) {
            _vp = vp;
            _srcVertexId = srcVertexId;
            _dstVertexId = dstVertexId;
            _dstIsGlobal = dstIsGlobal;
            _nOutgoingEdges = nOutgoingEdges;

            if (nOutgoingEdges>0) {
                _veip = vp._edgeIndex;
                _vertexRowId = vp._apm.getRowId(srcVertexId);
                _veip.findMatchingEdges(this);
                _index = _lastIndex;
            }
            else {
                _index = -1;
            }
        }


        /**
         * @return true if there are further edges within this iterator, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            for (; ; ) {
                if (_index==-1 || _index<_firstIndex) {
                    return false;
                }

                int rowId = _veip.getVertexRowIdByIndexRowId(_index);

                boolean isDstGlobal = _veip.getDstIsGlobalByRowId(rowId);
                if (isDstGlobal == _dstIsGlobal) {
                    long edgeId = _veip.getEdgeIdByRowId(rowId);
                    reset(edgeId);
                    --_index;
                    return true;
                }
                else {
                    --_index;
                }
            }
        }
    }
}
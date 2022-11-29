/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * This class provides iterators that can iterate over vertices in a relatively efficient manner.
 *<p>
 * Single-threaded and multi-threaded iterators are provided as well as some support for
 * random-access of vertices.
 *<p>
 * It encapsulates common functions such as getting fields and properties etc.
 *<p>
 * Some of the methods within this class return fields, properties and further iterators.
 * It should be noted that all of those objects will be re-used (or become invalid) if
 * the iterator moves to another vertex - meaning that you should use them immediately
 * and copy the value, if required.
 * <p>
 * <p>TODO: Check the use of thread-locals here. Some use is made of them to reduce memory consumption
 * <p>TODO: during heavy iteration. However, there may be better ways to do this - and it's quite likely
 * <p>TODO: that the memory consumption is pretty minor anyway.
 */
public abstract class VertexIterator {
    private static final ThreadLocal<WindowedVertexIterator> _wvsTL = ThreadLocal.withInitial(WindowedVertexIterator::new);
    private static final ThreadLocal<AllVerticesIterator> _aviTL = ThreadLocal.withInitial(AllVerticesIterator::new);

    /**
     * MTVertexIteratorConsumer - clients of the multi-thread vertex iterators will
     * need to implement this interface to receive thread-specific iterators.
     *
     * @see MTAllVerticesManager
     * @see MTWindowedVertexManager
     */
    public interface MTVertexIteratorConsumer {
        /**
         * Invoked by the multi-threaded manager once per vertex partition, in a separate thread.
         * The client can then iterate over this partition whilst other threads are iterating over
         * other partitions.
         *
         * @param partitionId the vertex partition id being iterated over
         * @param iter the iterator to use
         */
        public void accept(int partitionId, VertexIterator iter);
    }



    private boolean _hasNext = false;
    private boolean _getNext = true;

    protected VertexPartitionManager _avpm;
    protected VertexPartition _p;
    protected int _partitionId = -1;
    protected int _vertexRowId = -1;
    protected long _vertexId = -1L;
    protected EntityFieldAccessor[] _efa = null;
    protected VersionedEntityPropertyAccessor[] _vefa = null;
    protected ArrowPropertyIterator[] _propertyIterators = null;
    protected VersionedEntityPropertyAccessor[] _propertyIteratorAccessors = null;
    protected EdgeIterator.MatchingEdgesIterator _matchingEdgesIterator = null;
    protected VertexHistoryIterator.WindowedVertexHistoryIterator _vertexHistoryIterator = null;
    protected CachedMutatingEdgeMap.MatchingEdgeCachedIterator _scanner = null;
    protected VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator _isAliveSearcher = null;



    /**
     * Initialise this iterator
     *
     * @param avpm the vertex partition manager to use
     */
    protected void init(VertexPartitionManager avpm) {
        _avpm = avpm;
        _p = null;
        _partitionId = -1;
        _vertexRowId = -1;
        _vertexId = -1L;
        _hasNext = false;
        _getNext = true;
    }


    /**
     * Initialise this iterator
     *
     * @param avpm the vertex partition manager to use
     * @param p the partition we're iterating over
     */
    protected void init(VertexPartitionManager avpm, VertexPartition p) {
        _avpm = avpm;
        _p = p;
        _partitionId = _p.getPartitionId();
        _vertexRowId = -1;
        _vertexId = -1L;
        _hasNext = false;
        _getNext = true;
    }


    public RaphtoryArrowPartition getRaphtory() { return _avpm._raphtoryPartition; }


    public VertexPartition getPartition() {
        return _p;
    }

    /**
     * Retrieves a field within the current vertex directly
     *
     * @param field the field in question
     *
     * @return the field accessor that contains the value
     */
    public EntityFieldAccessor getField(int field) {
        if (_efa==null) {
            _efa = _avpm._raphtoryPartition.createVertexEntityFieldAccessors();
        }

        return _p.getFieldByVertexRow(field, _vertexRowId, _efa[field]);
    }


    /**
     * Retrieves a property within the current vertex directly
     *
     * @param field the property in question
     *
     * @return the property accessor that contains the value
     */
    public VersionedEntityPropertyAccessor getProperty(int field) {
        if (_vefa==null) {
            _vefa = _avpm._raphtoryPartition.createVertexEntityPropertyAccessors();
        }

        int propertyRow = _p.getPropertyPrevPtrByRow(field, _vertexRowId);
        if (propertyRow==-1) {
            return null;
        }

        return _p.getPropertyByPropertyRow(field, propertyRow, _vefa[field]);
    }


    /**
     * Resets this iterator - repositioning it at the specified vertex.
     *
     * @param vertexId the vertex-id to move to
     *
     * @return true if the specified vertex exists, false otherwise
     */
    public boolean reset(long vertexId) {
        _vertexId = vertexId;
        _vertexRowId = _avpm.getRowId(vertexId);
        _p = _avpm.getPartition(_avpm.getPartitionId(vertexId));
        _hasNext = _vertexId!=-1L && _p!=null && _p.isValidRow(_vertexRowId);
        _getNext = false;

        return _hasNext;
    }


    /**
     * @return the current vertex id
     */
    public long getVertexId() { return _vertexId; }


    /**
     * @return the next local-id of this vertex, or -1L if unavailable
     */
    public final long next() {
        if (_getNext) {
            moveToNext();
        }

        _getNext = true;

        return _vertexId;
    }


    /**
     * @return true if there are further vertices within this iterator, false otherwise
     */
    public final boolean hasNext() {
        if (_getNext) {
            _hasNext = moveToNext();
            _getNext = false;
        }

        return _hasNext;
    }


    /**
     * Moves to the next matching vertex in this iteration
     *
     * @return true iff there's a matching vertex, false otherwise
     */
    protected abstract boolean moveToNext();


    /**
     * Retrieves the current vertex as a POJO Vertex. The ref-count will be 1.
     *
     * @return the POJO Vertex
     */
    public Vertex getVertex() {
        return _p.getVertex(_vertexId);
    }


    /**
     * Returns whether the vertex is alive within the specified time window
     *
     * @param start the start time of the window (inclusive)
     * @param end the end time of the window (inclusive)
     *
     * @return true if the vertex is alive at the end of the window
     */
    public boolean isAliveAt(long start, long end) {
        if (_isAliveSearcher ==null) {
            _isAliveSearcher = new VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator();
        }

        return _p.isAliveAt(_vertexId, start, end, _isAliveSearcher);
    }


    /**
     * @return the creation time for the current vertex
     */
    public long getCreationTime() {
        return _p.getCreationTimeByRow(_vertexRowId);
    }


    /**
     * @return the global-vertex-id for the current vertex
     */
    public long getGlobalVertexId() {
        return _p._getGlobalVertexIdByRow(_vertexRowId);
    }


    /**
     * @return the number of incoming edges for the current vertex
     */
    public int getNIncomingEdges() {
        return _p._getNIncomingEdgesByRow(_vertexRowId);
    }


    /**
     * @return the number of outgoing edges for the current vertex
     */
    public int getNOutgoingEdges() {
        return _p._getNOutgoingEdgesByRow(_vertexRowId);
    }


    /**
     * The edge iterator returned here should be used immediately, once the vertex iterator moves
     * to a new vertex, the edge iterator will be re-used.
     *
     * @return an edge iterator, configured to iterate over the outgoing edges of the current vertex
     */
    public abstract EdgeIterator getOutgoingEdges();


    /**
     * The edge iterator returned here should be used immediately, once the vertex iterator moves
     * to a new vertex, the edge iterator will be re-used.
     *
     * @return an edge iterator, configured to iterate over the incoming edges of the current vertex
     */
    public abstract EdgeIterator getIncomingEdges();


    /**
     * The edge iterator returned here should be used immediately, once the vertex iterator moves
     * to a new vertex, the edge iterator will be re-used.
     *
     * @return an edge iterator, configured to iterate over both the incoming and outgoing edges of the current vertex
     */
    public abstract EdgeIterator getAllEdges();


    /**
     * @return true if the iterator is currently positioned at a valid vertex, false otherwise
     */
    public boolean isValid() {
        return _p!=null && _p.isValidRow(_vertexRowId);
    }


    /**
     * Retrieves all outgoing edges from this vertex to the specified
     * destination vertex id.
     *
     * @param dstVertexId the destination vertex id in question
     * @param isDstGlobal true if the dst-vertex-id is global, false otherwise
     *
     * @return an edge iterator configured to retrieve those edges
     * */
    public EdgeIterator findAllOutgoingEdges(long dstVertexId, boolean isDstGlobal) {
        // TODO XXX Find a better or more universal approach for this
        if (false) {
            // Use the sorted edges in the vertex partition
            // Seems to require sorting all the time as
            // edges etc are added...so slow!
            if (_matchingEdgesIterator == null) {
                _matchingEdgesIterator = new EdgeIterator.MatchingEdgesIterator();
            }

            _matchingEdgesIterator.init(_avpm._aepm);
            _matchingEdgesIterator.findEdgesViaVertex(_p, _vertexId, dstVertexId, isDstGlobal, getNOutgoingEdges());

            return _matchingEdgesIterator;
        }
        else {
            // Use a hash-map that's updated each time an edge is added
            if (_scanner==null) {
                _scanner = new CachedMutatingEdgeMap.MatchingEdgeCachedIterator();
            }

            return _p.findMatchingEdges(_vertexId, dstVertexId, isDstGlobal, _scanner);
        }
    }


    /**
     * Returns a property iterator for the specified property for the current vertex.
     * The returned iterator should be used immediately as it will be re-used if the
     * vertex iterator moves to another vertex.
     *
     * @param property the property in question
     *
     * @return the property-iterator
     */
    public ArrowPropertyIterator getPropertyHistory(int property) {
        if (_propertyIterators==null) {
            _propertyIteratorAccessors = _p._apm._raphtoryPartition.createVertexEntityPropertyAccessors();
            _propertyIterators = new ArrowPropertyIterator[_propertyIteratorAccessors.length];
            for (int i=0; i<_propertyIterators.length; ++i) {
                _propertyIterators[i] = new ArrowPropertyIterator.AllPropertiesIterator();
                _propertyIterators[i].init(_p._propertyStores[property]._store, _p._propertyStores[property]._accessor, _propertyIteratorAccessors[i], -1);
            }
        }

        _propertyIterators[property].init(_p._propertyStores[property]._store, _p._propertyStores[property]._accessor, _propertyIteratorAccessors[property], _p.getPropertyPrevPtrByRow(property, _vertexRowId));

        return _propertyIterators[property];
    }


    /**
     * @return A vertex history iterator, configured for the entire history of this vertex
     */
    public VertexHistoryIterator.WindowedVertexHistoryIterator getVertexHistory() {
        if (_vertexHistoryIterator==null) {
            _vertexHistoryIterator = new VertexHistoryIterator.WindowedVertexHistoryIterator();
        }

        _vertexHistoryIterator.init(_avpm, _vertexId, Long.MIN_VALUE, Long.MAX_VALUE);

        return _vertexHistoryIterator;
    }


    /**
     * This class iterates over active vertices within a time window
     * using the vertex history.
     *<p>
     * <p>TODO: It is expected that the implementation of this will change
     * <p>TODO: substantially once we start using snapshots.
     * <p>TODO: Confirm performance is good.
     */
    public static class WindowedVertexHistoryIterator extends VertexIterator {
        protected LongOpenHashSet _processedVertices = new LongOpenHashSet(1024);
        protected long _minTime;
        protected long _maxTime;

        protected int _firstIndex;
        protected int _lastIndex;
        protected int _index;
        protected boolean _firstTime = true;
        protected boolean _getNextPartition = false;
        protected long _modifiedTime = -1L;

        protected EdgeIterator.AllWindowedEdgeIteratorFromVertex _allEdges = new EdgeIterator.AllWindowedEdgeIteratorFromVertex();
        protected EdgeIterator.WindowedEdgeIteratorFromVertex _incomingEdges = new EdgeIterator.WindowedEdgeIteratorFromVertex();
        protected EdgeIterator.WindowedEdgeIteratorFromVertex _outgoingEdges = new EdgeIterator.WindowedEdgeIteratorFromVertex();


        private WindowedVertexHistoryIterator() {}


        /**
         * Instantiates this iterator.
         *
         * @param avpm the vertex partition manager to use
         * @param minTime the start time (inclusive) used for finding vertices
         * @param maxTime the end time (inclusive) used for finding vertices
         */
        protected WindowedVertexHistoryIterator(VertexPartitionManager avpm, long minTime, long maxTime) {
            init(avpm, minTime, maxTime);
        }


        /**
         * Initialises this iterator.
         *
         * @param avpm the vertex partition manager to use
         * @param minTime the start time (inclusive) used for finding vertices
         * @param maxTime the end time (inclusive) used for finding vertices
         */
        protected void init(VertexPartitionManager avpm, long minTime, long maxTime) {
            super.init(avpm);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;
            _index = -1;
            _getNextPartition = true;
        }



        /**
         * Initialises this iterator.
         *
         * @param avpm the vertex partition manager to use
         * @param p the specific vertex partition to use
         * @param minTime the start time (inclusive) used for finding vertices
         * @param maxTime the end time (inclusive) used for finding vertices
         */
        protected void init(VertexPartitionManager avpm, VertexPartition p, long minTime, long maxTime) {
            super.init(avpm, p);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;

            _firstIndex = -1;
            _lastIndex = -1;
            _index = -1;
            _processedVertices.clear();
            _getNextPartition = false;
        }


        /**
         * @return the vertex id of the next matching vertex
         */
        private long getNext() {
            for (;;) {
                if (_p==null && _getNextPartition) {
                    _p = _avpm.getPartition(++_partitionId);
                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _processedVertices.clear();
                    _vertexRowId = -1;
                    _vertexId = -1L;
                }

                if (_p == null) {
                    _vertexRowId = -1;
                    _vertexId = -1L;
                    return -1L;
                }

                if (_lastIndex!=-1) {
                    if (_index >= _firstIndex) {
                        //System.out.println("RET: " + _index);
                        int historyRowId = _p._history.getHistoryRowIdBySortedIndex(_index);
                        boolean alive = _p._history.getIsAliveByHistoryRowId(historyRowId);
                        _vertexRowId = _p._history.getVertexLocalRowIdByHistoryRowId(historyRowId);

                        //System.out.println("LOOKING AT: " + _index + ", " + _lastIndex + " -> " + _firstIndex + ", vr=" + _vertexRowId);
                        //System.out.println("LOOKING AT: " + _index + ", " + _lastIndex + " -> " + _firstIndex + ", vr=" + _vertexRowId + " -> " + _p._getLocalVertexIdByRow(_vertexRowId));

                        if (!_processedVertices.contains(_vertexRowId)) {
                            _processedVertices.add(_vertexRowId);
                            _modifiedTime = _p._history.getModificationTimeByHistoryRowId(historyRowId);
                            if (alive) {
                                return _p._getLocalVertexIdByRow(_vertexRowId);
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
                    _p = null;
                    _vertexRowId = -1;
                    _vertexId = -1L;
                }
                else {
                    _p._history.isAliveAtWithWindowVector(this);
                    if (_lastIndex==-1) {
                        _p = null;
                        _vertexRowId = -1;
                        _vertexId = -1L;
                    }
                    else {
                        _index = _lastIndex;
                    }
                    //System.out.println("WINDOW: " + _lastIndex + " -> " + _firstIndex);
                }
            }
        }


        /**
         * @return the modification time associated with this record
         */
        public long getModificationTime() { return _modifiedTime; }


        /**
         * @return true if there is another matching vertex, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (!_firstTime) {
                --_index;
            }
            else {
                _firstTime = false;
            }

            _vertexId = getNext();
            return _vertexId!=-1L;
        }


        /**
         * @return an edge iterator, iterating over the active outgoing edges for the current vertex
         * where the edges are active in the associated time window.
         */
        @Override
        public EdgeIterator getOutgoingEdges() {
            _outgoingEdges.init(_avpm._aepm, _p._getOutgoingEdgePtrByRow(_vertexRowId), false, _minTime, _maxTime);
            return _outgoingEdges;
        }


        /**
         * @return an edge iterator, iterating over the active incoming edges for the current vertex
         * where the edges are active in the associated time window.
         */
        @Override
        public EdgeIterator getIncomingEdges() {
            _incomingEdges.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), true, _minTime, _maxTime);
            return _incomingEdges;
        }


        /**
         * @return an edge iterator, iterating over all active edges for the current vertex
         * where the edges are active in the associated time window.
         */
        @Override
        public EdgeIterator getAllEdges() {
            _allEdges.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), _p._getOutgoingEdgePtrByRow(_vertexRowId), _minTime, _maxTime);
            return _allEdges;
        }


        /**
         * @return A vertex history iterator, configured for this time frame
         */
        @Override
        public VertexHistoryIterator.WindowedVertexHistoryIterator getVertexHistory() {
            if (_vertexHistoryIterator==null) {
                _vertexHistoryIterator = new VertexHistoryIterator.WindowedVertexHistoryIterator();
            }

            _vertexHistoryIterator.init(_avpm, _vertexId, _minTime, _maxTime);

            return _vertexHistoryIterator;
        }
    }


    /**
     * Base class for window-based edge-history iterators derived from a windowed vertex iterator.
     *<p>
     * It encapsulates common functions such as getting fields and properties etc.
     *<p>
     * Some of the methods within this class return fields and propertie.
     * It should be noted that all of those objects will be re-used (or become invalid) if
     * the iterator moves to another edge - meaning that you should use them immediately
     * and copy the value, if required.
     *<p>
     * This class is a little experimental.
     *<p>
     * <p>TODO: Check if we can merge this with EdgeIteratorFromVertex
     * <p>TODO: Test this thoroughly, appears to be a bit confused re class hierarchy
     * <p>TODO: Build a multi-threaded implementation?
     */
    public abstract static class WindowedVertexEdgeHistoryIterator extends EdgeIterator {
        protected LongOpenHashSet _processedEdges = new LongOpenHashSet(1024);

        protected int _vertexRowId;
        protected VertexPartition _vertexPartition;
        protected long _minTime;
        protected long _maxTime;
        protected int _maxEdges;

        // Edge specific fields...
        protected int _firstIndex;
        protected int _lastIndex;
        protected int _index;
        protected boolean _firstTime = true;
        protected int _nEdgesFound = 0;


        /**
         * Initialises this iterator
         *
         * @param wvs the originating windowed-vertex-iterator
         */
        protected void init(WindowedVertexHistoryIterator wvs) {
            super.init(wvs._avpm._aepm);

            _vertexRowId = wvs._vertexRowId;
            _vertexPartition = wvs._p;
            _minTime = wvs._minTime;
            _maxTime = wvs._maxTime;

            _firstIndex = -1;
            _lastIndex = -1;
            _index = -1;
            _firstTime = true;

            _processedEdges.clear();

            _maxEdges = getMaxEdges(_vertexRowId);
            _nEdgesFound = 0;
        }


        /**
         * Initialises this iterator
         *
         * @param vs the originating vertex-iterator
         * @param minTime the start time (inclusive) for matching edges
         * @param maxTime the end time (inclusive) for matching edges
         */
        protected void init(VertexIterator vs, long minTime, long maxTime) {
            super.init(vs._avpm._aepm);

            _vertexRowId = vs._vertexRowId;
            _vertexPartition = vs._p;
            _minTime = minTime;
            _maxTime = maxTime;

            _firstIndex = -1;
            _lastIndex = -1;
            _index = -1;
            _firstTime = true;

            _processedEdges.clear();

            _maxEdges = getMaxEdges(_vertexRowId);
            _nEdgesFound = 0;
        }


        /**
         * @return the next matching edge id, or -1 if there are no more matches
         */
        private long getNext() {
            for (;;) {
                if (_vertexPartition==null || _nEdgesFound>=_maxEdges) {
                    return -1L;
                }

                if (_lastIndex!=-1) {
                    if (_index >= _firstIndex) {
                        //System.out.println("RET: " + _index);
                        int historyRowId = _vertexPartition._history.getEdgeHistoryRowIdBySortedIndex(_index);
                        long edgeId = _vertexPartition._history.getEdgeIdByHistoryRowId(historyRowId);

                        //System.out.println("INDEX=" + _index + ", ROW=" + historyRowId + ", EDGE: " + edgeId);
                        if (edgeId!=-1L && !_processedEdges.contains(edgeId)) {
                            boolean alive = _vertexPartition._history.getIsAliveByHistoryRowId(historyRowId);
                            boolean isInteresting = isInteresting(historyRowId);

                            if (isInteresting) {
                                ++_nEdgesFound;
                                _processedEdges.add(edgeId);
                            }

                            if (alive && isInteresting) {
                                return edgeId;
                            }
                        }
                        --_index;
                        continue;
                    }

                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _vertexPartition = null;
                }
                else {
                    _vertexPartition._history.isAliveAtWithWindowVector(this);
                    if (_lastIndex==-1) {
                        _vertexPartition = null;
                    }
                    else {
                        _index = _lastIndex;
                    }
                    //System.out.println("WINDOW: " + _lastIndex + " -> " + _firstIndex);
                }
            }
        }


        /**
         * @return true if there are further matching edges, false otherwise
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
            if (_edgeId!=-1L) {
                _edgeRowId = _aepm.getRowId(_edgeId);
                _edgePartition = _aepm.getPartition(_aepm.getPartitionId(_edgeId));
                return true;
            }
            else {
                _edgeRowId = -1;
                _edgePartition = null;
                return false;
            }
        }


        /**
         * Filters the history for incoming, outgoing or all edges
         *
         * @param historyRowId the history row-id in question
         *
         * @return true if that history record matches the interest
         */
        protected abstract boolean isInteresting(int historyRowId);


        /**
         * Returns the maximum number of unique matching edges
         *
         * @param vertexRowId the vertex in question
         *
         * @return the maximum
         */
        protected abstract int getMaxEdges(int vertexRowId);
    }


    /**
     * Window-based edge iterator from a vertex - returning incoming edges
     */
    public final static class IncomingWindowedVertexEdgeHistoryIterator extends WindowedVertexEdgeHistoryIterator {
        /**
         * Filters in incoming edges only.
         *
         * @param historyRowId the history row-id in question
         *
         * @return true if it's an incoming edge, false otherwise
         */
        @Override
        protected boolean isInteresting(int historyRowId) {
            return !_vertexPartition._history.getIsOutgoingByHistoryRowId(historyRowId);
        }


        /**
         * Returns the maximum number of incoming edges for the vertex
         *
         * @param vertexRowId the vertex in question
         *
         * @return the maximum number of incoming edges
         */
        @Override
        public int getMaxEdges(int vertexRowId) {
            return _vertexPartition._getNIncomingEdgesByRow(_vertexRowId);
        }
    }


    /**
     * Window-based edge iterator from a vertex - returning outgoing edges
     */
    public final static class OutgoingWindowedVertexEdgeHistoryIterator extends WindowedVertexEdgeHistoryIterator {
        /**
         * Filters in outgoing edges only.
         *
         * @param historyRowId the history row-id in question
         *
         * @return true if it's an outgoing edge, false otherwise
         */
        @Override
        protected boolean isInteresting(int historyRowId) {
            return _vertexPartition._history.getIsOutgoingByHistoryRowId(historyRowId);
        }

        /**
         * Returns the maximum number of outgoing edges for the vertex
         *
         * @param vertexRowId the vertex in question
         *
         * @return the maximum number of outgoing edges
         */
        @Override
        public int getMaxEdges(int vertexRowId) {
            return _vertexPartition._getNOutgoingEdgesByRow(_vertexRowId);
        }
    }


    /**
     * Window-based edge iterator from a vertex - returning incoming and outgoing edges
     */
    public final static class AllWindowedVertexEdgeHistoryIterator extends WindowedVertexEdgeHistoryIterator {
        /**
         * Filters in all edges.
         *
         * @param historyRowId the history row-id in question
         *
         * @return true
         */
        @Override
        protected boolean isInteresting(int historyRowId) {
            return true;
        }


        /**
         * Returns the maximum number of incoming and outgoing edges for the vertex
         *
         * @param vertexRowId the vertex in question
         *
         * @return the maximum number of incoming and outgoing edges
         */
        @Override
        public int getMaxEdges(int vertexRowId) {
            return _vertexPartition._getNIncomingEdgesByRow(_vertexRowId) + _vertexPartition._getNOutgoingEdgesByRow(_vertexRowId);
        }
    }


    /**
     * Multi-threaded windowed vertex iterator manager
     *<p>
     * This class allows the iteration of vertices fitting a time window,
     * processing each vertex partition in a separate thread.
     */
    public static class MTWindowedVertexManager {
        /**
         * Internal iteration task class - invoked once per partition in a separate thread
         */
        private class IterTask implements Runnable {
            protected final VertexPartition _pp;
            protected final MTVertexIteratorConsumer _consumer;

            /**
             * Instantiates an IterTask
             *
             * @param p the vertex partition to iterate over
             * @param consumer the consumer to invoke
             */
            private IterTask(VertexPartition p, MTVertexIteratorConsumer consumer) {
                _pp = p;
                _consumer = consumer;
            }


            /**
             * Task run method - gets hold of a WindowedVertexHistoryIterator and invokes the consumer,
             * allowing it to iterate over matching vertices in it's own thread
             */
            @Override
            public void run() {
                WindowedVertexIterator iter = getCachedWindowedVertexIterator(_avpm, _pp, _minTime, _maxTime);
                _consumer.accept(_pp.getPartitionId(), iter);
            }
        }


        private final ArrayList<Future<?>> _tasks = new ArrayList<>();
        protected VertexPartitionManager _avpm;
        protected VertexPartition _p;
        protected long _minTime;
        protected long _maxTime;
        protected RaphtoryThreadPool _pool;


        /**
         * Initialise this instance.
         *
         * @param avpm vertex partition manager to use
         * @param pool thread pool to use
         * @param minTime the start time for searching (inclusive)
         * @param maxTime the end time for searching (inclusive)
         */
        public void init(VertexPartitionManager avpm, RaphtoryThreadPool pool, long minTime, long maxTime) {
            _avpm = avpm;
            _pool = pool;
            _minTime = minTime;
            _maxTime = maxTime;
            _p = null;
            _tasks.clear();
        }


        /**
         * Forces the caller to wait until the sub-tasks have completed.
         */
        public void waitTilComplete() {
            _pool.waitTilComplete(_tasks);
        }


        /**
         * Attempts to cancel the iteration process
         */
        public synchronized void cancel() {
            _pool.cancel(_tasks);
        }


        /**
         * Start the iteration - invoking the consumer in multiple threads
         *
         * @param consumer the consumer to invoke
         */
        public synchronized void start(MTVertexIteratorConsumer consumer) {
            int partitionId = 0;
            _tasks.clear();

            for (;;) {
                _p = _avpm.getPartition(partitionId++);
                if (_p==null) {
                    break;
                }

                IterTask task = new IterTask(_p, consumer);
                Future<?> f = _pool.submitTask(task);
                _tasks.add(f);
            }
        }
    }


    /**
     * Multi-threaded vertex iterator manager
     *<p>
     * This class allows the iteration of vertices processing each vertex partition in a separate thread.
     */
    public static class MTAllVerticesManager {
        private final ArrayList<Future<?>> _tasks = new ArrayList<>();
        protected VertexPartitionManager _avpm;
        protected RaphtoryThreadPool _pool;


        protected MTAllVerticesManager() {}

        /**
         * Initialise this instance
         *
         * @param avpm the vertex partition manager to use
         * @param pool the thread pool to use
         */
        protected void init(VertexPartitionManager avpm, RaphtoryThreadPool pool) {
            _avpm = avpm;
            _pool = pool;
        }


        /**
         * Start the iteration - invoking the consumer in multiple threads
         *
         * @param consumer the consumer to invoke
         */
        public synchronized void start(MTVertexIteratorConsumer consumer) {
            int partitionId = 0;
            _tasks.clear();

            for (;;) {
                VertexPartition p = _avpm.getPartition(partitionId++);
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
            protected final VertexPartition _pp;
            protected final MTVertexIteratorConsumer _consumer;

            /**
             * Instantiate the IterTask
             *
             * @param p the vertex partition to iterate over
             * @param consumer the consumer to invoke
             */
            private IterTask(VertexPartition p, MTVertexIteratorConsumer consumer) {
                _pp = p;
                _consumer = consumer;
            }


            /**
             * Task run method - gets hold of a AllVerticesIterator and invokes the consumer,
             * allowing it to iterate over the vertices in it's own thread
             */
            @Override
            public void run() {
                AllVerticesIterator iter = getCachedAllVerticesIterator(_avpm, _pp);
                _consumer.accept(_pp.getPartitionId(), iter);
            }
        }
    }


    /**
     * Iterator that iterates over all vertices
     *<p>
     * <p>TODO: Should the edge-iterating functionality be part of VertexIterator?
     */
    public static class AllVerticesIterator extends VertexIterator {
        private boolean _doAll;
        private final EdgeIterator.EdgeIteratorFromVertex _incoming = new EdgeIterator.EdgeIteratorFromVertex();
        private final EdgeIterator.EdgeIteratorFromVertex _outgoing = new EdgeIterator.EdgeIteratorFromVertex();
        private final EdgeIterator.AllEdgesIteratorFromVertex _allEdges = new EdgeIterator.AllEdgesIteratorFromVertex();


        private AllVerticesIterator() {}


        /**
         * Instantiates this iterator.
         *
         * @param avpm the vertex partition manager to use
         */
        protected AllVerticesIterator(VertexPartitionManager avpm) {
            init(avpm);
        }


        /**
         * Initialises this instance
         *
         * @param avpm the vertex partition manager to use
         */
        protected void init(VertexPartitionManager avpm) {
            super.init(avpm);
            _doAll = true;
        }


        /**
         * Initialises this instance
         *
         * @param avpm the vertex partition manager to use
         * @param p the single partition we're iterating over
         */
        protected void init(VertexPartitionManager avpm, VertexPartition p) {
            super.init(avpm, p);
            _doAll = false;
        }


        /**
         * @return true if this iterator has further matching vertices, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            for (;;) {
                if (_doAll && _p==null) {
                    _p = _avpm.getPartition(++_partitionId);
                    if (_p == null) {
                        _vertexRowId = -1;
                        _vertexId = -1L;
                        return false;
                    }
                    _vertexRowId = -1;
                    _vertexId = -1L;
                }

                if (_p==null) {
                    _vertexRowId = -1;
                    _vertexId = -1L;
                    return false;
                }

                if (_p.isValidRow(++_vertexRowId)) {
                    _vertexId = _p._getLocalVertexIdByRow(_vertexRowId);
                    return true;
                }

                _p = null;
                _vertexRowId = -1;
                _vertexId = -1L;
            }
        }


        /**
         * @return an edge iterator, iterating over the current vertex's outgoing edges
         */
        @Override
        public EdgeIterator getOutgoingEdges() {
            _outgoing.init(_avpm._aepm, _p._getOutgoingEdgePtrByRow(_vertexRowId), false);
            return _outgoing;
        }


        /**
         * @return an edge iterator, iterating over the current vertex's incoming edges
         */
        @Override
        public EdgeIterator getIncomingEdges() {
            _incoming.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), true);
            return _incoming;
        }


        /**
         * @return an edge iterator, iterating over all of the current vertex's edges
         */
        @Override
        public EdgeIterator getAllEdges() {
            _allEdges.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), _p._getOutgoingEdgePtrByRow(_vertexRowId));
            return _allEdges;
        }
    }


    /**
     * Returns a thread-local all-vertices iterator
     *
     * @param avpm vertex partition manager to use
     * @param p the specific vertex partition to use
     *
     * @return the thread-local iterator
     */
    protected static AllVerticesIterator getCachedAllVerticesIterator(VertexPartitionManager avpm, VertexPartition p) {
        AllVerticesIterator avi = _aviTL.get();
        avi.init(avpm, p);
        return avi;
    }


    /**
     * Returns a thread-local windowed-vertices iterator
     *
     * @param avpm vertex partition manager to use
     * @param p the specific vertex partition to use
     * @param minTime - the start time for the window (inclusive)
     * @param maxTime - the end time for the window (inclusive)
     *
     * @return the thread-local iterator
     */
    private static WindowedVertexIterator getCachedWindowedVertexIterator(VertexPartitionManager avpm, VertexPartition p, long minTime, long maxTime) {
        WindowedVertexIterator wvs = _wvsTL.get();
        wvs.init(avpm, p, minTime, maxTime);
        return wvs;
    }




    /**
     * Iterator that iterates over all vertices that are alive within a time window
     *<p>
     * <p>TODO: Should the edge-iterating functionality be part of VertexIterator?
     */
    public static class WindowedVertexIterator extends VertexIterator {
        private boolean _doAll;
        protected EdgeIterator.AllWindowedEdgeIteratorFromVertex _allEdges = new EdgeIterator.AllWindowedEdgeIteratorFromVertex();
        protected EdgeIterator.WindowedEdgeIteratorFromVertex _incomingEdges = new EdgeIterator.WindowedEdgeIteratorFromVertex();
        protected EdgeIterator.WindowedEdgeIteratorFromVertex _outgoingEdges = new EdgeIterator.WindowedEdgeIteratorFromVertex();
        protected VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator _searcher = new VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator();
        protected long _start;
        protected long _end;


        private WindowedVertexIterator() {}


        /**
         * Instantiates this iterator.
         *
         * @param avpm the vertex partition manager to use
         */
        protected WindowedVertexIterator(VertexPartitionManager avpm, long start, long end) {
            init(avpm, start, end);
        }


        /**
         * Initialises this instance
         *
         * @param avpm the vertex partition manager to use
         */
        protected void init(VertexPartitionManager avpm, long start, long end) {
            super.init(avpm);
            _doAll = true;
            _start = start;
            _end = end;
        }


        /**
         * Initialises this instance
         *
         * @param avpm the vertex partition manager to use
         */
        protected void init(VertexPartitionManager avpm, VertexPartition p, long start, long end) {
            super.init(avpm, p);
            _doAll = false;
            _start = start;
            _end = end;
        }



        /**
         * Initialises this instance
         *
         * @param avpm the vertex partition manager to use
         * @param p the single partition we're iterating over
         */
        protected void init(VertexPartitionManager avpm, VertexPartition p) {
            super.init(avpm, p);
            _doAll = false;
        }


        /**
         * @return true if this iterator has further matching vertices, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            for (;;) {
                if (_doAll && _p==null) {
                    _p = _avpm.getPartition(++_partitionId);
                    if (_p == null) {
                        _vertexRowId = -1;
                        _vertexId = -1L;
                        return false;
                    }
                    _vertexRowId = -1;
                    _vertexId = -1L;
                }

                if (_p==null) {
                    _vertexRowId = -1;
                    _vertexId = -1L;
                    return false;
                }

                while (_p.isValidRow(++_vertexRowId) && !_p.isAliveAtByRow(_vertexRowId, _start, _end, _searcher)) {
                    // NOP
                }

                if (_p.isValidRow(_vertexRowId)) {
                    _vertexId = _p._getLocalVertexIdByRow(_vertexRowId);
                    return true;
                }

                _p = null;
                _vertexRowId = -1;
                _vertexId = -1L;
            }
        }


        /**
         * @return an edge iterator, iterating over the current vertex's outgoing edges
         */
        @Override
        public EdgeIterator getOutgoingEdges() {
            _outgoingEdges.init(_avpm._aepm, _p._getOutgoingEdgePtrByRow(_vertexRowId), false, _start, _end);
            return _outgoingEdges;
        }


        /**
         * @return an edge iterator, iterating over the current vertex's incoming edges
         */
        @Override
        public EdgeIterator getIncomingEdges() {
            _incomingEdges.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), true, _start, _end);
            return _incomingEdges;
        }


        /**
         * @return an edge iterator, iterating over all of the current vertex's edges
         */
        @Override
        public EdgeIterator getAllEdges() {
            _allEdges.init(_avpm._aepm, _p._getIncomingEdgePtrByRow(_vertexRowId), _p._getOutgoingEdgePtrByRow(_vertexRowId), _start, _end);
            return _allEdges;
        }
    }
}
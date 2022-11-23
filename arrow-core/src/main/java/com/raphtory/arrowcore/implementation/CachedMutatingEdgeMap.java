package com.raphtory.arrowcore.implementation;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * This class is used to find edges between a source vertex
 * and a particular destination vertex.
 *
 * It's updated whenever edges are added and is also
 * updated lazily if queried.
 */
public class CachedMutatingEdgeMap {
    public static final int N_EDGES_FOR_MAP = 64;

    protected final Long2ObjectOpenHashMap<Long2ObjectOpenHashMap<LongArrayList>> _srcToEdgesMap = new Long2ObjectOpenHashMap<>();
    protected final VertexPartition _p;
    protected final EdgePartitionManager _aepm;
    protected final VertexPartitionManager _avpm;


    public CachedMutatingEdgeMap(VertexPartition p, VertexPartitionManager avpm, EdgePartitionManager aepm) {
        _p = p;
        _avpm = avpm;
        _aepm = aepm;
    }


    /**
     * Adds a new edge to the map
     *
     * @param srcId the src vertex id
     * @param dstId the dst vertex id
     * @param edgeId the edge id in question*
     * @param nSrcOutgoingEdges the number of outgoing edges in the src vertex
     */
    public void addEdge(long srcId, long dstId, long edgeId, int nSrcOutgoingEdges) {
        if (nSrcOutgoingEdges<N_EDGES_FOR_MAP) {
            return;
        }

        Long2ObjectOpenHashMap<LongArrayList> edgeMap = _srcToEdgesMap.get(srcId);
        boolean isNew = edgeMap==null;

        if (edgeMap==null) {
            edgeMap = new Long2ObjectOpenHashMap<>();
            _srcToEdgesMap.put(srcId, edgeMap);
        }

        LongArrayList edges = edgeMap.get(dstId);
        if (edges==null) {
            edges = new LongArrayList();
            edgeMap.put(dstId, edges);
        }

        if (isNew) {
            // Now add the other edges for this src..
            addAllEdges(srcId, edgeId, nSrcOutgoingEdges);
        }
        else {
            edges.add(edgeId);
        }
    }


    /**
     * Adds all of the edges for a vertex into the map
     *
     * @param srcId the src vertex id
     * @param edgeId the latest edge-id
     * @param nSrcOutgoingEdges the number of outgoing edges for the src vertex
     */
    private void addAllEdges(long srcId, long edgeId, int nSrcOutgoingEdges) {
        while (edgeId!=-1L) {
            int edgePartitionId = _aepm.getPartitionId(edgeId);
            int edgeRowId = _aepm.getRowId(edgeId);
            EdgePartition p = _aepm.getPartition(edgePartitionId);

            addEdge(srcId, p.getDstVertexId(edgeRowId), edgeId, nSrcOutgoingEdges);

            edgeId = p._getPrevOutgoingPtrByRow(edgeRowId);
        }
    }


    /**
     * Configures a matching-edges iterator to find matching edges as specified
     *
     * @param srcId the src vertex id
     * @param dstId the dst vertex id to find
     * @param dstIsGlobal true if the dst is global, false otherwise
     * @param nSrcOutgoingEdges the number of outgoing edges for this src vertex
     * @param iter the iterator to configure
     *
     * @return the configured iterator
     */
    public EdgeIterator findMatchingEdges(long srcId, long dstId, boolean dstIsGlobal, int nSrcOutgoingEdges, MatchingEdgeCachedIterator iter) {
        iter.init(_aepm);

        Long2ObjectOpenHashMap<LongArrayList> edgeMap = _srcToEdgesMap.get(srcId);
        if (edgeMap!=null) {
            // Use the existing map
            LongArrayList edges = edgeMap.get(dstId);
            iter.init(edges, dstIsGlobal);
        }
        else if (nSrcOutgoingEdges>N_EDGES_FOR_MAP) {
            // We should initialise the map and use it
            int row = _avpm.getRowId(srcId);
            addAllEdges(srcId, _p._getOutgoingEdgePtrByRow(row), nSrcOutgoingEdges);

            edgeMap = _srcToEdgesMap.get(srcId);
            LongArrayList edges = edgeMap.get(dstId);
            iter.init(edges, dstIsGlobal);
        }
        else {
            // Just iterate over the edge-list
            iter.init(_avpm, srcId, dstId, dstIsGlobal);
        }

        return iter;
    }


    /**
     * Edge iterator that iterates over edges from a particular source
     * to a particular destination.
     *
     * This iterator can either iterate over the linked-list of edges, where
     * the head is stored in the vertex store OR it can iterate over an array
     * of relevant edge-ids.
     */
    public static class MatchingEdgeCachedIterator extends EdgeIterator {
        private boolean _dstIsGlobal;
        private long _dstId;
        private long _nextEdgeId;
        private int _index = 0;
        private boolean _useList;
        private long[] _edges;
        private int _nEdges;


        /**
         * Initialises the iterator to iterate over the cached edge list
         *
         * @param edgeIds the cached edge list
         * @param dstIsGlobal true, if the dst vertex is global, false otherwise
         */
        public void init(LongArrayList edgeIds, boolean dstIsGlobal) {
            _useList = true;

            // We use the underlying array rather than the LongArrayList because
            // the LongArrayList may change whilst we're iterating over it
            if (edgeIds==null || edgeIds.size()==0) {
                _edges = null;
                _nEdges = 0;
            }
            else {
                _nEdges = edgeIds.size();
                _edges = edgeIds.elements();
            }

            _dstIsGlobal = dstIsGlobal;
        }


        /**
         * Initialises the iterator to iterate over the edge linked-list for the src vertex
         *
         * @param avpm the VertexPartitionManager to use
         * @param srcId the src vertex id
         * @param dstId the dst vertex id
         * @param dstIsGlobal true, if the dst vertex is global, false otherwise
         */
        public void init(VertexPartitionManager avpm, long srcId, long dstId, boolean dstIsGlobal) {
            _useList = false;
            //_avpm = avpm;
            _dstId = dstId;
            _dstIsGlobal = dstIsGlobal;

            int vPartId = avpm.getPartitionId(srcId);
            int vRowId = avpm.getRowId(srcId);

            _nextEdgeId = avpm.getPartition(vPartId)._getOutgoingEdgePtrByRow(vRowId);
        }


        /**
         * Moves to the next item in the iteration
         * @return true if there is a next item, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (_useList) {
                if (_nEdges==0) {
                    return false;
                }

                while (_index<_nEdges) {
                    boolean hasNext = reset(_edges[_index++]);
                    if (!hasNext) {
                        return false;
                    }

                    if (isDstVertexLocal() != _dstIsGlobal) {
                        return true;
                    }
                }

                return false;
            }

            while (_nextEdgeId!=-1L) {
                if (!reset(_nextEdgeId)) {
                    return false;
                }

                _nextEdgeId = _edgePartition._getPrevOutgoingPtrByRow(_edgeRowId);

                if (getDstVertexId()==_dstId && isDstVertexLocal()!=_dstIsGlobal) {
                    return true;
                }
            }
            return false;
        }
    }
}
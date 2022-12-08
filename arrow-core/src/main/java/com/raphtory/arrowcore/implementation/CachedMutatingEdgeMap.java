package com.raphtory.arrowcore.implementation;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * This class is used to find edges between a source vertex
 * and a particular destination vertex.
 *
 * It's updated whenever edges are added and is also
 * updated lazily if queried.
 *
 * TODO: Investigate the use of LongHashFunction.hashLongs() to hash
 * the src & dst ids to generate a KEY.
 * The KEY is then stored in the map against a EDGE-ID value.
 *
 * If multiple EDGE-IDs need to be stored for that KEY then the stored EDGE-ID
 * value becomes a -ve index into the edge-list list, which when we iterate
 * over it, we will need to filter on src and dst.
 *
 * This may use much less memory.
 */
public abstract class CachedMutatingEdgeMap {
    public static final int N_EDGES_FOR_MAP = 64;

    protected final VertexPartition _p;
    protected final EdgePartitionManager _aepm;
    protected final VertexPartitionManager _avpm;

    protected final Long2ObjectOpenHashMap<Long2LongOpenHashMap> _vertexToEdgeMapMap = new Long2ObjectOpenHashMap<>();
    protected final ObjectArrayList<LongArrayList> _edgeLists = new ObjectArrayList<>();



    public CachedMutatingEdgeMap(VertexPartition p, VertexPartitionManager avpm, EdgePartitionManager aepm) {
        _p = p;
        _avpm = avpm;
        _aepm = aepm;
    }


    /**
     * Adds a new edge to the map
     *
     * @param originatingVertexId the vertex id of the target of the edge (outgoing=src, incoming=dst)
     * @param otherVertexId the vertex id of the vertex at the other end of this edge
     * @param edgeId the edge id in question
     * @param prevEdgeId the prev edge in the list
     * @param nEdges the number of edges in the target vertex (outgoing edges or incoming edges as appropriate)
     */
    protected void addEdge(long originatingVertexId, long otherVertexId, long edgeId, long prevEdgeId, int nEdges) {
        if (nEdges<N_EDGES_FOR_MAP) {
            return;
        }

        Long2LongOpenHashMap edgeMap = _vertexToEdgeMapMap.get(originatingVertexId);
        boolean isNew = edgeMap==null;

        if (isNew) {
            edgeMap = new Long2LongOpenHashMap();
            edgeMap.defaultReturnValue(Long.MAX_VALUE);
            _vertexToEdgeMapMap.put(originatingVertexId, edgeMap);
            // Now add the other edges for this src...
            addAllEdges(edgeMap, otherVertexId, edgeId, prevEdgeId);
        }
        else {
            addEdge(edgeMap, edgeId, otherVertexId);
        }
    }


    /**
     * Adds all the edges for a vertex into the map
     *
     * @param edgeId the latest edge-id
     */
    private void addAllEdges(Long2LongOpenHashMap edgeMap, long vertexId, long edgeId, long prevEdgeId) {
        addEdge(edgeMap, edgeId, vertexId);

        addAllEdges(edgeMap, prevEdgeId);
    }


    protected abstract void addAllEdges(Long2LongOpenHashMap edgeMap, long prevEdgeId);

    protected abstract void addAllEdgesFromOriginatingVertex(Long2LongOpenHashMap edgeMap, long originatingVertexId);


    protected void addEdge(Long2LongOpenHashMap edgeMap, long edgeId, long otherVertexId) {
        long edgeIdOrListId = edgeMap.get(otherVertexId);

        if (edgeIdOrListId>=0L) {
            if (edgeIdOrListId == Long.MAX_VALUE) {
                // No mapping yet, so just map the edge-id
                edgeMap.put(otherVertexId, edgeId);
            }
            else {
                // An existing mapping, convert to a list
                long foundEdgeId = edgeIdOrListId;

                LongArrayList list = new LongArrayList();
                list.add(foundEdgeId);
                list.add(edgeId);

                _edgeLists.add(list);

                int listId = -_edgeLists.size();

                edgeMap.put(otherVertexId, listId);
            }
        }
        else {
            // An existing mapping to a list
            int listId = (int)(-edgeIdOrListId-1);
            LongArrayList list = _edgeLists.get(listId);
            list.add(edgeId);
        }
    }


    /**
     * Configures a matching-edges iterator to find matching edges as specified
     *
     * @param originatingVertexId the src vertex id
     * @param targetVertexId the dst vertex id to find
     * @param targetIsGlobal true if the dst is global, false otherwise
     * @param nEdges the number of outgoing edges for this src vertex
     * @param iter the iterator to configure
     *
     * @return the configured iterator
     */
    public EdgeIterator findMatchingEdges(long originatingVertexId, long targetVertexId, boolean targetIsGlobal, int nEdges, MatchingEdgeCachedIterator iter) {
        iter.init(_aepm);

        Long2LongOpenHashMap edgeMap = _vertexToEdgeMapMap.get(originatingVertexId);
        if (edgeMap!=null) {
            // Use the existing map
            long edgeIdOrListId = edgeMap.get(targetVertexId);
            if (edgeIdOrListId>=0L) {
                iter.init(edgeIdOrListId);
            }
            else {
                int listId = (int)(-edgeIdOrListId-1);
                iter.init(_edgeLists.get(listId), targetIsGlobal);
            }
        }
        else if (nEdges>=N_EDGES_FOR_MAP) {
            // Initialise the mappings for this src
            edgeMap = new Long2LongOpenHashMap();
            edgeMap.defaultReturnValue(Long.MAX_VALUE);
            _vertexToEdgeMapMap.put(originatingVertexId, edgeMap);

            // Now add the other edges for this src...
            addAllEdgesFromOriginatingVertex(edgeMap, originatingVertexId);

            long edgeIdOrListId = edgeMap.get(targetVertexId);
            if (edgeIdOrListId>=0L) {
                iter.init(edgeIdOrListId);
            }
            else {
                int listId = (int)(-edgeIdOrListId-1);
                iter.init(_edgeLists.get(listId), targetIsGlobal);
            }
        }
        else {
            // Just iterate over the edge-list
            iter.init(_avpm, originatingVertexId, targetVertexId, targetIsGlobal);
        }

        return iter;
    }


    public static class OutgoingCachedMutatingEdgeMap extends CachedMutatingEdgeMap {
        public OutgoingCachedMutatingEdgeMap(VertexPartition p, VertexPartitionManager avpm, EdgePartitionManager aepm) {
            super(p, avpm, aepm);
        }


        @Override
        protected void addAllEdges(Long2LongOpenHashMap edgeMap, long prevEdgeId) {
            while (prevEdgeId != -1L) {
                int edgePartitionId = _aepm.getPartitionId(prevEdgeId);
                int edgeRowId = _aepm.getRowId(prevEdgeId);
                EdgePartition p = _aepm.getPartition(edgePartitionId);

                addEdge(edgeMap, prevEdgeId, p.getDstVertexId(edgeRowId));
                prevEdgeId = p._getPrevOutgoingPtrByRow(edgeRowId);
            }
        }

        @Override
        protected void addAllEdgesFromOriginatingVertex(Long2LongOpenHashMap edgeMap, long originatingVertexId) {
            int row = _avpm.getRowId(originatingVertexId);
            addAllEdges(edgeMap, _p._getOutgoingEdgePtrByRow(row));
        }
    }


    public static class IncomingCachedMutatingEdgeMap extends CachedMutatingEdgeMap {
        public IncomingCachedMutatingEdgeMap(VertexPartition p, VertexPartitionManager avpm, EdgePartitionManager aepm) {
            super(p, avpm, aepm);
        }


        @Override
        protected void addAllEdges(Long2LongOpenHashMap edgeMap, long prevEdgeId) {
            while (prevEdgeId!=-1L) {
                int edgePartitionId = _aepm.getPartitionId(prevEdgeId);
                int edgeRowId = _aepm.getRowId(prevEdgeId);
                EdgePartition p = _aepm.getPartition(edgePartitionId);

                addEdge(edgeMap, prevEdgeId, p.getSrcVertexId(edgeRowId));
                prevEdgeId = p._getPrevIncomingPtrByRow(edgeRowId);
            }
        }


        @Override
        protected void addAllEdgesFromOriginatingVertex(Long2LongOpenHashMap edgeMap, long originatingVertexId) {
            int row = _avpm.getRowId(originatingVertexId);
            addAllEdges(edgeMap, _p._getIncomingEdgePtrByRow(row));
        }
    }


    /**
     * Edge iterator that iterates over edges from a particular source
     * to a particular destination.
     *
     * This iterator can either iterate over the linked-list of edges, where
     * the head is stored in the vertex store OR it can iterate over an array
     * of relevant edge-ids.
     */
    public static abstract class MatchingEdgeCachedIterator extends EdgeIterator {
        protected boolean _isGlobal;
        protected long _vertexId;
        protected long _nextEdgeId;
        protected int _index = 0;
        protected boolean _useEdgeArrayList;
        protected long[] _edges;
        protected int _nEdges;


        protected void init(long edgeId) {
            if (edgeId==Long.MAX_VALUE) {
                _edgeId = -1L;
                _edgeRowId = -1;
                _edgePartition = null;
                _hasNext = false;
                _getNext = false;
            }
            else {
                reset(edgeId);
            }

            _useEdgeArrayList = false;
            _nEdges = 0;
        }


        /**
         * Initialises the iterator to iterate over the cached edge list
         *
         * @param edgeIds the cached edge list
         * @param isGlobal true, if the other vertex is global, false otherwise
         */
        protected void init(LongArrayList edgeIds, boolean isGlobal) {
            _useEdgeArrayList = true;
            _index = 0;

            if (edgeIds==null || edgeIds.size()==0) {
                _edges = null;
                _nEdges = 0;
            }
            else {
                // We use the underlying array rather than the LongArrayList because
                // the LongArrayList may change whilst we're iterating over it
                _nEdges = edgeIds.size();
                _edges = edgeIds.elements();
            }

            _isGlobal = isGlobal;
        }


        /**
         * Initialises the iterator to iterate over the edge linked-list for the appropriate vertex
         *
         * @param avpm the VertexPartitionManager to use
         * @param vertexId the vertex id from where we get the edge linked list
         * @param targetVertexId the vertex id to look for
         * @param targetIsGlobal true, if the target vertex is global, false otherwise
         */
        protected void init(VertexPartitionManager avpm, long vertexId, long targetVertexId, boolean targetIsGlobal) {
            _useEdgeArrayList = false;
            _vertexId = targetVertexId;
            _isGlobal = targetIsGlobal;

            int vPartId = avpm.getPartitionId(vertexId);
            int vRowId = avpm.getRowId(vertexId);

            _nextEdgeId = getHeadEdgePtr(avpm.getPartition(vPartId), vRowId);
        }



        /**
         * Moves to the next item in the iteration
         * @return true if there is a next item, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (_useEdgeArrayList) {
                if (_nEdges==0) {
                    return false;
                }

                while (_index<_nEdges) {
                    boolean hasNext = reset(_edges[_index++]);
                    if (!hasNext) {
                        return false;
                    }

                    if (localFlagMatches()) {
                        return true;
                    }
                }

                return false;
            }

            // Scan the linked list of edge-ptrs
            while (_nextEdgeId!=-1L) {
                if (!reset(_nextEdgeId)) {
                    return false;
                }

                _nextEdgeId = goToNextEdge();

                if (vertexAndLocalFlagMatches()) {
                    return true;
                }
            }

            return false;
        }


        protected abstract long getHeadEdgePtr(VertexPartition p, int rowId);


        protected abstract boolean localFlagMatches();


        protected abstract boolean vertexAndLocalFlagMatches();


        protected abstract long goToNextEdge();
    }



    public static class OutgoingMatchingEdgeCachedIterator extends MatchingEdgeCachedIterator {
        @Override
        protected long getHeadEdgePtr(VertexPartition p, int rowId) { // XXX
            return p._getOutgoingEdgePtrByRow(rowId);
        }


        @Override
        protected boolean localFlagMatches() {
            return isDstVertexLocal() != _isGlobal;
        }


        @Override
        protected boolean vertexAndLocalFlagMatches() {
            return getDstVertexId()==_vertexId && isDstVertexLocal()!=_isGlobal;
        }


        @Override
        protected long goToNextEdge() {
            return _edgePartition._getPrevOutgoingPtrByRow(_edgeRowId);
        }
    }


    public static class IncomingMatchingEdgeCachedIterator extends MatchingEdgeCachedIterator {
        @Override
        protected long getHeadEdgePtr(VertexPartition p, int rowId) { // XXX
            return p._getIncomingEdgePtrByRow(rowId);
        }


        @Override
        protected boolean localFlagMatches() {
            return isSrcVertexLocal() != _isGlobal;
        }


        @Override
        protected boolean vertexAndLocalFlagMatches() {
            return getSrcVertexId()==_vertexId && isSrcVertexLocal()!=_isGlobal;
        }


        @Override
        protected long goToNextEdge() {
            return _edgePartition._getPrevIncomingPtrByRow(_edgeRowId);
        }
    }
}
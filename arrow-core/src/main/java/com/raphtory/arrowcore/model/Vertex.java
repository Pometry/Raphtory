/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.model;

import com.raphtory.arrowcore.implementation.*;

import java.util.ArrayList;

/**
 * Experimental class representing a vertex.
 *<p>
 * This is reference-counted and pooled to reduce memory usage
 * when dealing with large data-sets.
 * Instances of this class can be used to hold various values and then
 * have them all added/updated in the actual store in one fell swoop.
 *<p>
 * TODO: This class will be removed as most use cases are available via
 * iterators or via direct calls into the VertexPartitionManager and
 * as such this class contains a number of unused methods.
 *<p>
 * @see VertexIterator
 * @see VertexPartitionManager
 */
public class Vertex extends Entity {
    protected VertexIterator _vertexIterator;
    protected int _nIncomingEdges;
    protected int _nOutgoingEdges;
    protected long _incomingEdgePtr = -1L;
    protected long _outgoingEdgePtr = -1L;


    /**
     * @param rap RaphtoryArrowPartition associated with this vertex
     * @param fieldAccessors the field accessors to use
     * @param propertyAccessors the property accessors to use
     */
    public Vertex(RaphtoryArrowPartition rap, EntityFieldAccessor[] fieldAccessors, VersionedEntityPropertyAccessor[] propertyAccessors) {
        super(rap, fieldAccessors, propertyAccessors);
    }


    /**
     * Decrements the ref-count. If it goes to zero then
     * the vertex is cleared and put back into the pool
     * for re-use.
     */
    @Override
    public void decRefCount() {
        if (--_refCount == 0) {
            reset();
            _rap.putVertex(this);
        }
    }


    /**
     * @return the number of incoming edges
     */
    public int nIncomingEdges() { return _nIncomingEdges; }


    /**
     * @return the number of outgoing edges
     */
    public int nOutgoingEdges() { return _nOutgoingEdges; }


    /**
     * @return the head of the incoming list of edges
     */
    public long getIncomingEdgePtr() { return _incomingEdgePtr; }


    /**
     * @return the head of the outgoing list of edges
     */
    public long getOutgoingEdgePtr() { return _outgoingEdgePtr; }


    /**
     * @return an edge-iterator set to retrieve the incoming edges for this vertex
     */
    public EdgeIterator getIncomingEdges() {
        if (_vertexIterator==null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);
        return _vertexIterator.getIncomingEdges();
    }


    /**
     * @return an edge-iterator set to retrieve the outgoing edges for this vertex
     */
    public EdgeIterator getOutgoingEdges() {
        if (_vertexIterator==null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);
        return _vertexIterator.getOutgoingEdges();
    }


    /**
     * @return an edge-iterator set to retrieve all edges for this vertex
     */
    public EdgeIterator getAllEdges() {
        if (_vertexIterator==null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);
        return _vertexIterator.getAllEdges();
    }


    /**
     * Resets the contents of this object
     */
    @Override
    public void clear() {
        super.clear();

        _nIncomingEdges = 0;
        _nOutgoingEdges = 0;
        _incomingEdgePtr = -1L;
        _outgoingEdgePtr = -1L;
    }


    /**
     * Recycles this object by recycling the incoming and outgoing edges
     */
    @Override
    public void recycle() {
        super.recycle();
        _nIncomingEdges = 0;
        _nOutgoingEdges = 0;
        _incomingEdgePtr = -1L;
        _outgoingEdgePtr = -1L;
    }


    /**
     * Resets the basic history details in this vertex (along with any edge data)
     *
     * @param localId this vertexId
     * @param globalId the vertex's global-id
     * @param initialValue true if this vertex is active at this time, false otherwise
     * @param time the time of this historical point
     */
    @Override
    public void reset(long localId, long globalId, boolean initialValue, long time) {
        super.reset(localId, globalId, initialValue, time);
    }


    /**
     * Resets the edge data for this vertex
     *
     * @param incomingEdgePtr the edgeId at the head of the incoming edges list for this vertex
     * @param outgoingEdgePtr the edgeId at the head of the outgoing edges list for this vertex
     * @param nIncomingEdges the number of edges in the incoming edge list
     * @param nOutgoingEdges the number of edges in the outgoing edge list
     */
    public void resetEdgeData(long incomingEdgePtr, long outgoingEdgePtr, int nIncomingEdges, int nOutgoingEdges) {
        _incomingEdgePtr = incomingEdgePtr;
        _outgoingEdgePtr = outgoingEdgePtr;
        _nIncomingEdges = nIncomingEdges;
        _nOutgoingEdges = nOutgoingEdges;
    }


    /**
     * Clones an edge
     *
     * @param e edge to clone
     * @return the clone of edge e
     */
    protected Edge makeClone(Edge e) {
        Edge newOne = _rap.getEdge();
        e.makeClone(newOne);
        return newOne;
    }


    /**
     * Clones a vertex
     *
     * @param newOne the vertex that this vertex is cloned into
     * @return newOne (the resultant cloned vertex)
     */
    public Vertex makeClone(Vertex newOne) {
        super.makeClone(newOne);

        newOne._nIncomingEdges = _nIncomingEdges;
        newOne._nOutgoingEdges = _nOutgoingEdges;

        return newOne;
    }


    @Override
    public String toString() {
        return "Vertex: "+ _globalId;
    }


    /**
     * Returns a property iterator for the specified property for this vertex.
     *
     * @param property the property in question
     *
     * @return the property-iterator
     */
    public ArrowPropertyIterator getPropertyHistory(int property) {
        if (_vertexIterator == null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);
        return _vertexIterator.getPropertyHistory(property);
    }


    /**
     * Returns an edge iterator configured to iterate over all
     * edges having a src equal to this vertex and a dst
     * equal to the specified dst-vertex-id
     *
     * @param dstVertexId the dst vertex id in question
     * @param dstIsGlobal true if the dest vertex id is global, false otherwise
     *
     * @return the configured edge iterator
     */
    public EdgeIterator findAllOutgoingEdges(long dstVertexId, boolean dstIsGlobal) {
        if (_vertexIterator == null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);

        return _vertexIterator.findAllOutgoingEdges(dstVertexId, dstIsGlobal);
    }

    /**
     * Returns an edge iterator configured to iterate over all
     * edges having a src equal to this vertex and a dst
     * equal to the specified dst-vertex-id
     *
     * @param srcVertexId the dst vertex id in question
     * @param isSrcGlobal true if the dest vertex id is global, false otherwise
     *
     * @return the configured edge iterator
     */
    public EdgeIterator findAllIncomingEdges(long srcVertexId, boolean isSrcGlobal) {
        if (_vertexIterator == null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);

        return _vertexIterator.findAllIncomingEdges(srcVertexId, isSrcGlobal);
    }


    /**
     * Returns an edge iterator configured to iterate over all
     * edges having a dst equal to this vertex and a src
     * equal to the specified src-vertex-id
     *
     * @param srcVertexId the dst vertex id in question
     * @param srcIsGlobal true if the dest vertex id is global, false otherwise
     *
     * @return the configured edge iterator
     */
    public EdgeIterator findAllIncomingEdges(long srcVertexId, boolean srcIsGlobal) {
        if (_vertexIterator == null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);

        return _vertexIterator.findAllIncomingEdges(srcVertexId, srcIsGlobal);
    }


    /**
     * @return A vertex history iterator, configured for the entire history of this vertex
     */
    public VertexHistoryIterator getVertexHistory() {
        if (_vertexIterator == null) {
            _vertexIterator = _rap.getNewAllVerticesIterator();
        }

        _vertexIterator.reset(_localId);

        return _vertexIterator.getVertexHistory();
    }
}
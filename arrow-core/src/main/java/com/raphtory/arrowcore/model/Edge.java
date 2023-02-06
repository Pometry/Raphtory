/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.model;

import com.raphtory.arrowcore.implementation.*;

/**
 * Experimental class representing an edge.
 *<p>
 * This is reference-counted and pooled to reduce memory usage
 * when dealing with large data-sets.
 * Instances of this class can be used to hold various values and then
 * have them all added/updated in the actual store in one fell swoop.
 *<p>
 * <p>TODO: This class will be removed as most use cases are available via
 * iterators or via direct calls into the EdgePartitionManager and
 * as such this class contains a number of unused methods.
 *
 * @see EdgeIterator
 * @see EdgePartitionManager
 */
public class Edge extends Entity {
    protected boolean _srcIsGlobal;
    protected boolean _dstIsGlobal;
    protected long _src;                // Local or global src-vertex-id (_srcIsGlobal tells us which)
    protected long _dst;                // Local or global dst-vertex-id (_dstIsGlobal tells us which)
    protected long _prevIncomingPtr = -1L;
    protected long _prevOutgoingPtr = -1L;
    protected EdgeIterator _edgeIterator;


    /**
     * @param rap RaphtoryArrowPartition associated with this vertex
     * @param fieldAccessors the field accessors to use
     * @param propertyAccessors the property accessors to use
     */
    public Edge(RaphtoryArrowPartition rap, EntityFieldAccessor[] fieldAccessors, VersionedEntityPropertyAccessor[] propertyAccessors) {
        super(rap, fieldAccessors, propertyAccessors);
    }


    /**
     * Intialises the edge instance with it's basic details.
     *
     * @param localId the edge's local id
     * @param initialValue true if this edge is active, false otherwise
     * @param time time associated with this edge
     */
    public void init(long localId, boolean initialValue, long time) {
        super.init(localId, localId, initialValue, time);
    }


    /**
     * Decrements the ref-count. If it goes to zero then
     * the edge is cleared and put back into the pool
     * for re-use.
     */
    @Override
    public void decRefCount() {
        if (--_refCount == 0) {
            _rap.putEdge(this);
        }
    }


    /**
     * @return the dst-vertex id for this edge
     */
    public long getDstVertex() { return _dst; }


    /**
     * @return the src-vertex id for this edge
     */
    public long getSrcVertex() { return _src; }


    /**
     * @return the local edge-id of the previous edge in this list of incoming edges
     */
    public long getPrevIncomingPtr() { return _prevIncomingPtr; }


    /**
     * @return the local edge-id of the previous edge in this list of outgoing edges
     */
    public long getPrevOutgoingPtr() { return _prevOutgoingPtr; }


    /**
     * Resets the contents of this object
     */
    @Override
    public void clear() {
        super.clear();

        _src = -1L;
        _dst = -1L;
        _prevOutgoingPtr = -1L;
        _prevIncomingPtr = -1L;
    }


    /**
     * Recycles this object by recycling the incoming and outgoing edges
     */
    @Override
    public void recycle() {
        super.recycle();
    }


    /**
     * Sets the basic edge data - used when retrieving edges and adding edges
     *
     * @param srcId local or global src-vertex-id
     * @param dstId local or global dst-vertex-id
     * @param prevIncomingList the previous edge id for the incoming list (ie. the old head of the list)
     * @param prevOutgoingList the previous edge id for the outgoing list (ie. the old head of the list)
     * @param srcIsGlobal true, if the src-vertex-id is a global id, false otherwise
     * @param dstIsGlobal true, if the dst-vertex-id is a global id, false otherwise
     */
    public void resetEdgeData(long srcId, long dstId, long prevIncomingList, long prevOutgoingList, boolean srcIsGlobal, boolean dstIsGlobal) {
        _src = srcId;
        _dst = dstId;
        this._srcIsGlobal = srcIsGlobal;
        this._dstIsGlobal = dstIsGlobal;
        this._prevIncomingPtr = prevIncomingList;
        this._prevOutgoingPtr = prevOutgoingList;
    }


    /**
     * Sets the basic edge data - used when adding edges
     *
     * @param srcId local or global src-vertex-id
     * @param dstId local or global dst-vertex-id
     * @param srcIsGlobal true, if the src-vertex-id is a global id, false otherwise
     * @param dstIsGlobal true, if the dst-vertex-id is a global id, false otherwise
     */
    public void resetEdgeData(long srcId, long dstId, boolean srcIsGlobal, boolean dstIsGlobal) {
        _src = srcId;
        _dst = dstId;
        this._srcIsGlobal = srcIsGlobal;
        this._dstIsGlobal = dstIsGlobal;
        this._prevIncomingPtr = -1L;
        this._prevOutgoingPtr = -1L;
    }



    /**
     * Clones this edge into newOne
     * The usage and implementation of this needs to be verified!
     *
     * @param newOne the edge that will be modified
     */
    protected void makeClone(Edge newOne) {
        super.makeClone(newOne);

        newOne._srcIsGlobal = _srcIsGlobal;
        newOne._dstIsGlobal = _dstIsGlobal;
        newOne._src = _src;
        newOne._dst = _dst;
    }


    /**
     * @return true if this is a split edge, false otherwise.
     */
    public boolean isSplitEdge() {
        return _srcIsGlobal || _dstIsGlobal;
    }


    /**
     * @return true if the src-vertex-id is a global-id
     */
    public boolean isSrcGlobal() {
        return _srcIsGlobal;
    }


    /**
     * @return true if the dst-vertex-id is a global-id
     */
    public boolean isDstGlobal() {
        return _dstIsGlobal;
    }


    @Override
    public String toString() {
        return "Edge: " + _localId + ": src=" + _src + ", dst=" + _dst;
    }


    /**
     * Returns a property iterator for the specified property for this edge.
     *
     * @param property the property in question
     *
     * @return the property-iterator
     */
    public ArrowPropertyIterator getPropertyHistory(int property) {
        if (_edgeIterator == null) {
            _edgeIterator = _rap.getNewAllEdgesIterator();
        }

        _edgeIterator.reset(_localId);
        return _edgeIterator.getPropertyHistory(property);
    }
}
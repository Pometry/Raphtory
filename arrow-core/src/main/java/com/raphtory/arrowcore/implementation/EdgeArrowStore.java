/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Edge;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.List;

/**
 * EdgeArrowStore - this class encapsulates the Arrow Schema used
 * for Edges.
 *<p>
 * Fields are provided for direct access to the individual arrow vectors
 * as well as methods for more complex interactions.
 */
public class EdgeArrowStore {
    /**
     * @return a list containing the base fields required for an arrow edge schema
     */
    public static List<Field> createEdgeFields() {
        List<Field> edgeFields = new ArrayList<>();
        edgeFields.add(new Field("local_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        edgeFields.add(new Field("src_vertex_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        edgeFields.add(new Field("dst_vertex_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        edgeFields.add(new Field("src_is_global", new FieldType(false, new ArrowType.Bool(), null), null));
        edgeFields.add(new Field("dst_is_global", new FieldType(false, new ArrowType.Bool(), null), null));
        edgeFields.add(new Field("initial_value", new FieldType(false, new ArrowType.Bool(), null), null));
        edgeFields.add(new Field("creation_time", new FieldType(false, new ArrowType.Int(64, true), null), null));
        edgeFields.add(new Field("oldest_point", new FieldType(false, new ArrowType.Int(64, true), null), null));
        edgeFields.add(new Field("prev_inc_edge_ptr", new FieldType(false, new ArrowType.Int(64, true), null), null)); // Points to previous edge
        edgeFields.add(new Field("prev_out_edge_ptr", new FieldType(false, new ArrowType.Int(64, true), null), null)); // Points to previous edge
        edgeFields.add(new Field("history_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null)); // Points to end of list...

        return edgeFields;
    }


    protected int _partitionId;             // Arrow partition-id for this instance
    protected VectorSchemaRoot _edgeRoot;   // Arrow root for this instance

    // The individual vectors that make up this schema
    protected BigIntVector _localIds;
    protected BigIntVector _srcVertexIds;
    protected BigIntVector _dstVertexIds;
    protected BitVector _srcIsGlobals;
    protected BitVector _dstIsGlobals;
    protected BitVector _initialValues;
    protected BigIntVector _creationTimes;
    protected BigIntVector _oldestPoints;
    protected BigIntVector _prevIncomingEdgesPtr;
    protected BigIntVector _prevOutgoingEdgesPtr;
    protected IntVector _historyPtr;

    // Accessors which can retrieve user-defined fields from the schema
    protected SchemaFieldAccessor[] _accessors;
    protected int _nAccessors;



    /**
     * Initialises this instance
     *
     * @param partitionId the Arrow partition-id
     * @param edgeRoot the Arrow schema-root containing the vectors
     * @param accessors the schema-field accessors for user-defined fields within this schema
     */
    protected void init(int partitionId, VectorSchemaRoot edgeRoot, SchemaFieldAccessor[] accessors) {
        _partitionId = partitionId;
        _edgeRoot = edgeRoot;
        _accessors = accessors;
        _nAccessors = _accessors==null ? 0 : _accessors.length;

        if (edgeRoot!=null) {
            _localIds = (BigIntVector) _edgeRoot.getVector("local_id");
            _srcVertexIds = (BigIntVector) _edgeRoot.getVector("src_vertex_id");
            _dstVertexIds = (BigIntVector) _edgeRoot.getVector("dst_vertex_id");
            _srcIsGlobals = (BitVector)_edgeRoot.getVector("src_is_global");
            _dstIsGlobals = (BitVector)_edgeRoot.getVector("dst_is_global");
            _initialValues = (BitVector) _edgeRoot.getVector("initial_value");
            _creationTimes = (BigIntVector) _edgeRoot.getVector("creation_time");
            _oldestPoints = (BigIntVector) _edgeRoot.getVector("oldest_point");
            _prevIncomingEdgesPtr = (BigIntVector) _edgeRoot.getVector("prev_inc_edge_ptr");
            _prevOutgoingEdgesPtr = (BigIntVector) _edgeRoot.getVector("prev_out_edge_ptr");
            _historyPtr = (IntVector)_edgeRoot.getVector("history_ptr");
        }
        else {
            _localIds = null;
            _srcVertexIds = null;
            _dstVertexIds = null;
            _srcIsGlobals = null;
            _dstIsGlobals = null;
            _initialValues = null;
            _creationTimes = null;
            _oldestPoints = null;
            _prevIncomingEdgesPtr = null;
            _prevOutgoingEdgesPtr = null;
            _historyPtr = null;
        }
    }


    /**
     * Adds or updates an edge
     *
     * @param row the Arrow row where this edge is to be stored
     * @param e the edge itself
     * @param prevIncomingPtr the previous incoming list pointer to use
     * @param prevOutgoingPtr the previous outgoing list pointer to use
     *
     * @return true if this is a new edge, false otherwise
     */
    protected boolean addEdge(int row, Edge e, long prevIncomingPtr, long prevOutgoingPtr) {
        //System.out.println("ADDING EDGE: " + row + " -> " + e);
        boolean isNew = _localIds.isSet(row) == 0;

        if (isNew) {
            _localIds.set(row, e.getLocalId());
            _srcVertexIds.set(row, e.getSrcVertex());
            _dstVertexIds.set(row, e.getDstVertex());
            _srcIsGlobals.set(row, e.isSrcGlobal() ? 1 : 0);
            _dstIsGlobals.set(row, e.isDstGlobal() ? 1 : 0);
        }

        _initialValues.set(row, e.getInitialValue() ? 1 : 0);
        _creationTimes.set(row, e.getCreationTime());
        _oldestPoints.set(row, e.getOldestPoint());

        _prevIncomingEdgesPtr.set(row, prevIncomingPtr);
        _prevOutgoingEdgesPtr.set(row, prevOutgoingPtr);

        // Set the user-defined schema fields
        for (int i=0;i<_nAccessors; ++i) {
            _accessors[i].store(e.getField(i), row);
        }

        return isNew;
    }


    /**
     * Retrieves an edge from the Arrow schema
     *
     * @param row the Arrow row to get the edge from
     * @param retEdge the edge object that's to be updated with this row's data
     *
     * @return retEdge
     */
    protected Edge retrieveEdge(int row, Edge retEdge) {
        // Set the abase details
        long id = _localIds.get(row);
        retEdge.reset(id, id, _initialValues.get(row) != 0, _creationTimes.get(row));

        retEdge.resetEdgeData(_srcVertexIds.get(row), _dstVertexIds.get(row), _prevIncomingEdgesPtr.get(row), _prevOutgoingEdgesPtr.get(row),
                _srcIsGlobals.get(row)!=0, _dstIsGlobals.get(row)!=0);

        // Set history pointer, if there is some
        retEdge.resetHistoryData(_historyPtr.get(row));

        // Immediately, set the value of the user-defined fields in this edge
        for (int i=0; i<_nAccessors; ++i) {
            _accessors[i].load(retEdge.getField(i), row); // TODO XXX Make this lazy (on request)
        }

        return retEdge;
    }


    /**
     * Sets the history pointer for this row, without having to
     * raise the vertex into a POJO Edge.
     *
     * @param row to set
     * @param historyPtr the value to set
     */
    public void setHistoryPtr(int row, int historyPtr) {
        _historyPtr.set(row, historyPtr);
    }


    /**
     * Returns the history pointer for this row, without having to
     * raise the vertex into a POJO Edge.
     *
     * @param row to look at
     *
     * @return the value of the history pointer (head of the list)
     */
    public int getHistoryPtr(int row) {
        if (_historyPtr.isSet(row)!=0) {
            return _historyPtr.get(row);
        }

        return -1;
    }
}
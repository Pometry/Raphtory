/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Vertex;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;

/**
 * VertexArrowStore - this class encapsulates the Arrow Schema used
 * for Vertices.
 *<p>
 * Fields are provided for direct access to the individual arrow vectors
 * as well as methods for more complex interactions.
 */
public class VertexArrowStore {
    /**
     * @return a list containing the base fields required for an arrow vertex schema
     */
    public static ArrayList<Field> createVertexFields() {
        ArrayList<Field> vertexFields = new ArrayList<>();

        vertexFields.add(new Field("global_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        vertexFields.add(new Field("initial_value", new FieldType(false, new ArrowType.Bool(), null), null));
        vertexFields.add(new Field("creation_time", new FieldType(false, new ArrowType.Int(64, true), null), null));
        vertexFields.add(new Field("oldest_point", new FieldType(false, new ArrowType.Int(64, true), null), null));

        vertexFields.add(new Field("n_incoming_edges", new FieldType(false, new ArrowType.Int(32, true), null), null));
        vertexFields.add(new Field("n_outgoing_edges", new FieldType(false, new ArrowType.Int(32, true), null), null));
        vertexFields.add(new Field("incoming_edges_ptr", new FieldType(false, new ArrowType.Int(64, true), null), null)); // Points to end of list...
        vertexFields.add(new Field("outgoing_edges_ptr", new FieldType(false, new ArrowType.Int(64, true), null), null)); // Points to end of list...
        vertexFields.add(new Field("history_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null)); // Points to end of list...

        return vertexFields;
    }


    protected VertexPartition _partition;   // Arrow vertex partition for this instance
    protected VectorSchemaRoot _vertexRoot; // Arrow root for this instance

    // The individual vectors that make up this schema
    protected BigIntVector _globalIds;
    protected BitVector _initialValues;
    protected BigIntVector _creationTimes;
    protected BigIntVector _oldestPoints;

    protected IntVector _nIncomingEdges;
    protected IntVector _nOutgoingEdges;
    protected BigIntVector _incomingEdges;
    protected BigIntVector _outgoingEdges;
    protected IntVector _historyPtr;

    // Accessors which can retrieve user-defined fields from the schema
    protected SchemaFieldAccessor[] _accessors;
    protected int _nAccessors;


    /**
     * Initialises this instance
     *
     * @param partition the Arrow vertex partition
     * @param vertexRoot the Arrow schema-root containing the vectors
     * @param accessors the schema-field accessors for user-defined fields within this schema
     */
    protected void init(VertexPartition partition, VectorSchemaRoot vertexRoot, SchemaFieldAccessor[] accessors) {
        _partition = partition;
        _vertexRoot = vertexRoot;
        _accessors = accessors;
        _nAccessors = _accessors==null ? 0 : _accessors.length;

        if (vertexRoot!=null) {
            _globalIds = (BigIntVector)_vertexRoot.getVector("global_id");
            _initialValues = (BitVector)_vertexRoot.getVector("initial_value");
            _creationTimes = (BigIntVector)_vertexRoot.getVector("creation_time");
            _oldestPoints = (BigIntVector)_vertexRoot.getVector("oldest_point");

            _nIncomingEdges = (IntVector)_vertexRoot.getVector("n_incoming_edges");
            _nOutgoingEdges = (IntVector)_vertexRoot.getVector("n_outgoing_edges");
            _incomingEdges = (BigIntVector)_vertexRoot.getVector("incoming_edges_ptr");
            _outgoingEdges = (BigIntVector)_vertexRoot.getVector("outgoing_edges_ptr");
            _historyPtr = (IntVector)_vertexRoot.getVector("history_ptr");
        }
        else {
            _globalIds = null;
            _initialValues = null;
            _creationTimes = null;
            _oldestPoints = null;

            _nIncomingEdges = null;
            _nOutgoingEdges = null;
            _incomingEdges = null;
            _outgoingEdges = null;
            _historyPtr = null;
        }
    }


    /**
     * Adds or updates a vertex
     *
     * @param row the Arrow row where this vertex is to be stored
     * @param v the vertex itself
     * @return true if this is a new vertex, false otherwise
     */
    public boolean addVertex(int row, Vertex v) {
        //System.out.println("Added vertex: " + v.getLocalId());

        int nthVertex = row;
        boolean isNew = _globalIds.isSet(nthVertex) == 0;

        if (isNew) {
            // These fields are only ever set once
            _globalIds.set(nthVertex, v.getGlobalId());

            // No history so far
            _historyPtr.set(nthVertex, -1);
        }

        // Set specific schema values
        _initialValues.set(nthVertex, v.getInitialValue() ? 1 : 0);
        _creationTimes.set(nthVertex, v.getCreationTime());
        _oldestPoints.set(nthVertex, v.getOldestPoint());
        _incomingEdges.set(nthVertex, v.getIncomingEdgePtr());
        _outgoingEdges.set(nthVertex, v.getOutgoingEdgePtr());
        _nIncomingEdges.set(nthVertex, v.nIncomingEdges());
        _nOutgoingEdges.set(nthVertex, v.nOutgoingEdges());

        // Set the user-defined schema fields
        for (int i=0; i<_nAccessors; ++i) {
            _accessors[i].store(v.getField(i), row);
        }

        return isNew;
    }


    /**
     * Retrieves a vertex from the Arrow schema
     *
     * @param row the Arrow row to get the vertex from
     * @param retVertex the vertex object that's to be updated with this row's data
     *
     * @return retVertex
     */
    protected Vertex retrieveVertex(int row, Vertex retVertex) {
        // Set the base details
        retVertex.reset(_partition._getLocalVertexIdByRow(row), _globalIds.get(row), _initialValues.get(row) != 0, _creationTimes.get(row));

        // Set the edge summary and the heads of the incoming and outgoing edge lists.
        retVertex.resetEdgeData(_incomingEdges.get(row), _outgoingEdges.get(row), _nIncomingEdges.get(row), _nOutgoingEdges.get(row));

        // Set pointer to history, if there is some
        if (_historyPtr.isSet(row)!=0) {
            retVertex.resetHistoryData(_historyPtr.get(row));
        }

        // Immediately, set the value of the user-defined fields in this vertex
        for (int i=0; i<_nAccessors; ++i) {
            _accessors[i].load(retVertex.getField(i), row); // TODO XXX Make this lazy (on request)
        }

        return retVertex;
    }


    /**
     * Returns the history pointer for this row, without having to
     * raise the vertex into a POJO Vertex.
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


    /**
     * Sets the history pointer for this row, without having to
     * raise the vertex into a POJO Vertex.
     *
     * @param row to set
     * @param historyPtr the value to set
     */
    public void setHistoryPtr(int row, int historyPtr) {
        _historyPtr.set(row, historyPtr);
    }
}
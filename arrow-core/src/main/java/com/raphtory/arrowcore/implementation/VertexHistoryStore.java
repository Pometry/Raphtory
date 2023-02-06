/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * VertexHistoryStore - this class encapsulates the Arrow Schema used
 * for the history of vertices.
 *<p>
 * Fields are provided for direct access to the individual arrow vectors
 * as well as methods for more complex interactions.
 */
public class VertexHistoryStore {
    protected static Schema HISTORY_SCHEMA = new Schema(createHistoryFields());


    /**
     * @return a list containing the arrow fields for the vertex history store
     */
    protected static List<Field> createHistoryFields() {
        List<Field> historyFields = new ArrayList<>();

        historyFields.add(new Field("vertex_row", new FieldType(false, new ArrowType.Int(32, true), null), null));
        historyFields.add(new Field("time", new FieldType(false, new ArrowType.Int(64, true), null), null));
        historyFields.add(new Field("active", new FieldType(false, new ArrowType.Bool(), null), null));
        historyFields.add(new Field("update", new FieldType(false, new ArrowType.Bool(), null), null));
        historyFields.add(new Field("prev_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null));
        historyFields.add(new Field("sorted_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));

        // These will contain edge-ids if this history record is related to an edge
        historyFields.add(new Field("edge_id", new FieldType(true, new ArrowType.Int(64, true), null), null));
        historyFields.add(new Field("is_outgoing", new FieldType(false, new ArrowType.Bool(), null), null));
        historyFields.add(new Field("sorted_vertex_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));

        //historyFields.add(new Field("snapshot", new FieldType(false, new ArrowType.Int(32, true), null), null));
        return historyFields;
    }


    protected IntVector    _vertexRowIds;               // row number of this vertex in the VertexPartition
    protected BigIntVector _times;                      // history time
    protected BitVector    _states;                     // active, inactive
    protected BitVector    _updates;                    // true iff a property was updated
    protected IntVector    _prevPtrs;                   // Pointer to previous history for this vertex
    protected IntVector    _sortedTimeIndices;          // Indexes to history records, sorted just by time
    protected IntVector    _sortedVertexTimeIndices;    // Indexes to history records, sorted by vertexRowId and then time
    protected BigIntVector _edgeIds;                    // The edge-id associated with this history record, if any
    protected BitVector    _isOutgoings;                // True, if the edge is an outgoing edge, false otherwise
    protected IntVector    _snapshots;                  // Pointer to the appropriate row in the snapshot

    protected VectorSchemaRoot _root;
    protected int _maxRow = 0;


    /**
     * Initialises this instance
     *
     * @param historyRoot the Arrow schema-root containing the vectors
     */
    public void init(VectorSchemaRoot historyRoot) {
        _root = historyRoot;
        if (_root != null) {
            _vertexRowIds = (IntVector) _root.getVector("vertex_row");
            _times = (BigIntVector) _root.getVector("time");
            _states = (BitVector) _root.getVector("active");
            _updates = (BitVector) _root.getVector("update");
            _prevPtrs = (IntVector) _root.getVector("prev_ptr");
            _sortedVertexTimeIndices = (IntVector) _root.getVector("sorted_vertex_time_index");
            _sortedTimeIndices = (IntVector) _root.getVector("sorted_time_index");
            _edgeIds = (BigIntVector)_root.getVector("edge_id");
            _isOutgoings = (BitVector) _root.getVector("is_outgoing");
            //_snapshots = (IntVector) _root.getVector("snapshot");
            _snapshots = null;
        }
        else {
            _vertexRowIds = null;
            _times = null;
            _states = null;
            _updates = null;
            _prevPtrs = null;
            _sortedTimeIndices = null;
            _sortedVertexTimeIndices = null;
            _edgeIds = null;
            _isOutgoings = null;
            _snapshots = null;
        }
    }


    /**
     * Adds a history record to the Arrow file
     *
     * @param vertexRow the row in the vertex partition where the owning vertex is stored
     * @param time the time of this history point
     * @param active true if the vertex was active at that time, false otherwise
     * @aparm updated true if a property was updated
     * @param prevPtr the prev-ptr, pointing to the next history record for this vertex
     * @param localEdgeId the associated edge, if appropriate or -1
     * @param outgoing true if the edge is an outgoing edge, false otherwise
     * @param snapshotRow the row number in the snapshot file for this time/history point
     *
     * @return the row in the history file where this record is stored
     */
    public int addHistory(int vertexRow, long time, boolean active, boolean updated, int prevPtr, long localEdgeId, boolean outgoing, int snapshotRow) {
        int row = _maxRow++;

        while (!_vertexRowIds.isSafe(row)) {
            _vertexRowIds.reAlloc();
            _times.reAlloc();
            _states.reAlloc();
            _updates.reAlloc();
            _prevPtrs.reAlloc();

            _edgeIds.reAlloc();
            _isOutgoings.reAlloc();
        }

        _vertexRowIds.set(row, vertexRow);
        _times.set(row, time);
        _states.set(row, active ? 1 : 0);
        _updates.set(row, updated ? 1 : 0);
        _prevPtrs.set(row, prevPtr);

        _edgeIds.set(row, localEdgeId);
        _isOutgoings.set(row, outgoing ? 1: 0);

        //_snapshots.setSafe(row, snapshotRow);

        return row;
    }


    /**
     * Returns the prev-ptr value at the specified row
     *
     * @param row the row in question
     *
     * @return the value of the previous pointer
     */
    public long getPrevPtr(int row) {
        return _prevPtrs.get(row);
    }
}

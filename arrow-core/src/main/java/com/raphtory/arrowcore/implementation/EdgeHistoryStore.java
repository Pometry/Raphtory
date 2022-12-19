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
 * EdgeHistoryStore - this class encapsulates the Arrow Schema used
 * for the history of eges.
 *<p>
 * Fields are provided for direct access to the individual arrow vectors
 * as well as methods for more complex interactions.
 */
public class EdgeHistoryStore {
    protected static Schema HISTORY_SCHEMA = new Schema(createHistoryFields());


    /**
     * @return a list containing the arrow fields for the edge history store
     */
    protected static List<Field> createHistoryFields() {
        List<Field> historyFields = new ArrayList<>();

        historyFields.add(new Field("edge_row", new FieldType(false, new ArrowType.Int(32, true), null), null));
        historyFields.add(new Field("time", new FieldType(false, new ArrowType.Int(64, true), null), null));
        historyFields.add(new Field("active", new FieldType(false, new ArrowType.Bool(), null), null));
        historyFields.add(new Field("update", new FieldType(false, new ArrowType.Bool(), null), null));
        historyFields.add(new Field("prev_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null));

        historyFields.add(new Field("sorted_edge_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));
        historyFields.add(new Field("sorted_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));
        return historyFields;
    }


    protected IntVector    _edgeRowIds;
    protected BigIntVector _times;
    protected BitVector    _states;
    protected BitVector    _updates;
    protected IntVector    _prevPtrs;
    protected IntVector    _sortedTimeIndices;
    protected IntVector    _sortedEdgeTimeIndices;
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
            _edgeRowIds = (IntVector) _root.getVector("edge_row");
            _times = (BigIntVector) _root.getVector("time");
            _states = (BitVector) _root.getVector("active");
            _updates = (BitVector) _root.getVector("update");
            _prevPtrs = (IntVector) _root.getVector("prev_ptr");
            _sortedEdgeTimeIndices = (IntVector) _root.getVector("sorted_edge_time_index");
            _sortedTimeIndices = (IntVector) _root.getVector("sorted_time_index");
        }
        else {
            _edgeRowIds = null;
            _times = null;
            _states = null;
            _updates = null;
            _prevPtrs = null;
            _sortedTimeIndices = null;
            _sortedEdgeTimeIndices = null;
        }
    }

    /**
     * Adds a history record to the Arrow file
     *
     * @param edgeRow the row in the edge partition where the owning edge is stored
     * @param time the time of this history point
     * @param active true if the vertex was active at that time, false otherwise
     * @param updated true if a property was updated
     * @param prevPtr the prev-ptr, pointing to the next history record for this edge
     *
     * @return the row in the history file where this record is stored
     */
    public int addHistory(int edgeRow, long time, boolean active, boolean updated, int prevPtr) {
        int row = _maxRow++;

        while (!_edgeRowIds.isSafe(row)) {
            _edgeRowIds.reAlloc();
            _times.reAlloc();
            _states.reAlloc();
            _updates.reAlloc();
            _prevPtrs.reAlloc();
        }

        _edgeRowIds.set(row, edgeRow);
        _times.set(row, time);
        _states.set(row, active ? 1 : 0);
        _updates.set(row, updated ? 1 : 0);
        _prevPtrs.set(row, prevPtr);

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

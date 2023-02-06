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

/**
 * VertexEdgeIndexArrowStore - this class encapsulates the Arrow Schema used
 * for indexing vertices and their edges for quick searching.
 * <p>
 * Note, the index only contains outgoing edges.
 */
public class VertexEdgeIndexArrowStore {
    protected static Schema INDEX_SCHEMA = new Schema(createIndexFields());


    /**
     * @return a list containing the base fields required for this vertex-edge index schema
     */
    public static ArrayList<Field> createIndexFields() {
        ArrayList<Field> fields = new ArrayList<>();

        fields.add(new Field("vertex_row", new FieldType(false, new ArrowType.Int(32, true), null), null));
        fields.add(new Field("dst_vertex_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        fields.add(new Field("edge_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        fields.add(new Field("dst_is_global", new FieldType(false, new ArrowType.Bool(), null), null));

        fields.add(new Field("sorted_vertex_dst_index", new FieldType(false, new ArrowType.Int(32, true), null), null));

        return fields;
    }


    protected int _partitionId;             // Arrow partition-id for this instance
    protected VectorSchemaRoot _indexRoot;  // Arrow root for this instance
    protected int _maxRow = 0;              // Number of rows in this index

    // The individual vectors that make up this schema
    protected IntVector _vertexRowIds;
    protected BigIntVector _dstVertexIds;
    protected BigIntVector _edgeIds;
    protected BitVector _isGlobals;
    protected IntVector _sortedIndex;


    /**
     * Initialises this instance
     *
     * @param partitionId the Arrow partition-id
     * @param indexRoot the Arrow schema-root containing the vectors
     */
    protected void init(int partitionId, VectorSchemaRoot indexRoot) {
        _partitionId = partitionId;
        _indexRoot = indexRoot;

        if (indexRoot!=null) {
            _vertexRowIds = (IntVector) _indexRoot.getVector("vertex_row");
            _dstVertexIds = (BigIntVector) _indexRoot.getVector("dst_vertex_id");
            _edgeIds = (BigIntVector) _indexRoot.getVector("edge_id");
            _sortedIndex = (IntVector) _indexRoot.getVector("sorted_vertex_dst_index");
            _isGlobals = (BitVector) _indexRoot.getVector("dst_is_global");
        }
        else {
            _vertexRowIds = null;
            _dstVertexIds = null;
            _edgeIds = null;
            _sortedIndex = null;
            _isGlobals = null;
        }
    }


    /**
     * Adds an entry
     *
     * @param vertexRowId the row id of the source vertex
     * @param dstId the destination vertex id
     * @poram edgeId the relevant edge id
     * @param dstIsGlobal true if the dst vertex is global, false otherwise
     */
    public void addIndexRecord(int vertexRowId, long dstId, long edgeId, boolean dstIsGlobal) {
        int row = _maxRow++;

        while (!_vertexRowIds.isSafe(row)) {
            _vertexRowIds.reAlloc();
            _dstVertexIds.reAlloc();
            _edgeIds.reAlloc();
            _sortedIndex.reAlloc();
            _isGlobals.reAlloc();
        }

        _vertexRowIds.set(row, vertexRowId);
        _dstVertexIds.set(row, dstId);
        _edgeIds.set(row, edgeId);
        _sortedIndex.set(row, row);
        _isGlobals.set(row, dstIsGlobal ? 1 : 0);
    }


    /**
     * Returns the edge id for this row
     *
     * @param row to look at
     *
     * @return the edge id
     */
    public long getEdgeId(int row) {
        if (_edgeIds.isSet(row)!=0) {
            return _edgeIds.get(row);
        }

        return -1L;
    }
}

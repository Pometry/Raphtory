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

public class VertexSnapshotStore {
    /**
     * @return a list containing the base fields required for an arrow vertex schema
     */
    public static ArrayList<Field> createSnapshotFields() {
        ArrayList<Field> fields = new ArrayList<>();

        fields.add(new Field("local_id", new FieldType(false, new ArrowType.Int(64, true), null), null)); // Make 32-bit?
        fields.add(new Field("initial_value", new FieldType(false, new ArrowType.Bool(), null), null));
        fields.add(new Field("time", new FieldType(false, new ArrowType.Int(64, true), null), null));

        fields.add(new Field("sorted_vertex_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));

        return fields;
    }


    protected VectorSchemaRoot _root;
    protected int _maxRow = 0;
    protected int _nProperties;
    protected IntVector[] _snapPtrs;
    protected VertexPartition _p;
    protected BigIntVector _localIds;
    protected BitVector _initialValues;
    protected BigIntVector _times;
    protected IntVector _sortedVertexTimeIndices;


    public VertexSnapshotStore() {
    }


    public void init(VectorSchemaRoot root, VertexPartition p) {
        _root = root;
        _p = p;

        ArrayList<VersionedProperty> props = p._apm._raphtoryPartition._propertySchema.versionedVertexProperties();
        _nProperties = props.size();
        if (_nProperties > 0) {
            _snapPtrs = new IntVector[_nProperties];
        }

        if (_root==null) {
            _localIds = null;
            _initialValues = null;
            _times = null;
            _sortedVertexTimeIndices = null;
            for (int i=0; i<_nProperties; ++i) {
                _snapPtrs[i] = null;
            }
        }
        else {
            _localIds = (BigIntVector) root.getVector("local_id");
            _initialValues = (BitVector) root.getVector("initial_value");
            _times = (BigIntVector) root.getVector("time");
            _sortedVertexTimeIndices = (IntVector)root.getVector("sorted_vertex_time_index");

            for (int i=0; i<_nProperties; ++i) {
                _snapPtrs[i] = (IntVector)_root.getVector("prop_prev_ptr_" + i);
            }
        }
    }


    public Schema createVertexSnapshotSchema() {
        ArrayList<VersionedProperty> properties = _p._apm._raphtoryPartition._propertySchema.versionedVertexProperties();
        if (properties!=null && properties.size()>0) {
            return null;
        }

        ArrayList<Field> fields = createSnapshotFields();

        int n = properties.size();
        for (int i=0; i<n; ++i) {
            VersionedProperty f = properties.get(i);
            Field prevPtrField = new Field("prop_prev_ptr_" + i, new FieldType(false, new ArrowType.Int(32, true), null), null);
            fields.add(prevPtrField);
        }

        return new Schema(fields);
    }


    public int takeSnapshot(long localVertexId, int vertexRow, boolean iv, long time, VertexPartition p) {
        int targetRow = _maxRow++;

        _localIds.setSafe(targetRow, localVertexId);
        _initialValues.setSafe(targetRow, iv ? 1 : 0);
        _times.setSafe(targetRow, time);

        for (int i=0; i<_nProperties; ++i) {
            _snapPtrs[i].setSafe(targetRow, p.getPropertyPrevPtrByRow(i, vertexRow));
        }

        return targetRow;
    }
}
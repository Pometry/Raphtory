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

import java.util.ArrayList;
import java.util.List;

/**
 * This class manages the actual Arrow schema for a property.
 */
public class PropertyStore {
    /**
     * @return the base (non-specialized) schema for a property
     */
    public static List<Field> createBasePropertySchema() {
        List<Field> propertyFields = new ArrayList<>();

        // local_id could be an int?
        propertyFields.add(new Field("local_id", new FieldType(false, new ArrowType.Int(64, true), null), null));
        propertyFields.add(new Field("initial_value", new FieldType(false, new ArrowType.Bool(), null), null));
        propertyFields.add(new Field("creation_time", new FieldType(false, new ArrowType.Int(64, true), null), null));
        propertyFields.add(new Field("prev_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null));

        return propertyFields;
    }


    protected VectorSchemaRoot _entityRoot;
    protected BigIntVector _localIds;
    protected BitVector _initialValues;
    protected BigIntVector _creationTimes;
    protected IntVector _prevPtrs;
    protected int _maxRow = 0;

    protected VersionedPropertyStore _accessor;

    protected int _partitionId;

    /**
     * Initialises this instance
     *
     * @param partitionId the partition-id of the associated vertex/edge partition
     * @param entityRoot the vector schema root forthis partition
     * @param accessor the property schema accessor for this property
     */
    protected void init(int partitionId, VectorSchemaRoot entityRoot, VersionedPropertyStore accessor) {
        _partitionId = partitionId;
        _entityRoot = entityRoot;
        _accessor = accessor;

        if (_entityRoot !=null) {
            _localIds = (BigIntVector) _entityRoot.getVector("local_id");
            _initialValues = (BitVector) _entityRoot.getVector("initial_value");
            _creationTimes = (BigIntVector) _entityRoot.getVector("creation_time");
            _prevPtrs = (IntVector) _entityRoot.getVector("prev_ptr");
        }
        else {
            _localIds = null;
            _initialValues = null;
            _creationTimes = null;
            _prevPtrs = null;
        }

        _accessor.initialise(entityRoot);
    }


    /**
     * Adds a new property value to this Arrow file
     *
     * @param localId the owning entity's local-id
     * @param initialValue true if the property was active, false otherwise
     * @param creationTime the associated time
     * @param prevPtr the prev-ptr pointing to the list of property values for this entity
     * @param ea the entity property accessor to use
     *
     * @return the row in which the new property was stored
     */
    protected int addProperty(long localId, boolean initialValue, long creationTime, int prevPtr, VersionedEntityPropertyAccessor ea) {
        int row = _maxRow++;

        _localIds.setSafe(row, localId);
        _initialValues.setSafe(row, initialValue ? 1 : 0);
        _creationTimes.setSafe(row, creationTime);
        _prevPtrs.setSafe(row, prevPtr);
        _accessor.store(ea, row);

        return row;
    }


    /**
     * Retrieves a property into the supplied entity property accessor
     *
     * @param row the row to read
     * @param ea the entity property accessor to update
     */
    public void loadProperty(int row, VersionedEntityPropertyAccessor ea) {
        _accessor.load(ea, row);
    }
}
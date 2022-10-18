/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

/**
 * This class interacts with EntityFieldAccessor to
 * enable user-defined fields to be created and used by entities.
 *<p>
 * The SchemaFieldAccessor encapsulates the Arrow specific
 * bits of the access - the schema creation, accessing using row etc.
 *<p>
 * References to the actual arrow vectors are stored here for faster access.
 *<p>
 * The only state that should be held by instances and sub-classes of this
 * class is the actual Arrow vector, as there will be 1 instance of this
 * for each field in each arrow file and multiple threads may be accessing
 * the same Arrow file concurrently.
 *<p>
 * The concrete sub-classes of SchemaFieldAccessor perform the
 * actual storage and retrieval via Arrow, and also load the value into
 * the appropriate EntityFieldAccessor.
 *<p>
 * Properties differ from Fields in Raphtory because a property's value can
 * change over time and the changes are tracked (history). A field is assumed
 * to be set once and doesn't change.
 *<p>
 * <p>TODO: Add support for enum fields
 *
 * @see EntityFieldAccessor
 */
public abstract class SchemaFieldAccessor {
    protected int _fieldIndex;


    /**
     * Initialises the basic schema details for this field.
     *
     * @param root the Arrow schema to retrieve fields from
     * @param fieldIndex the field-id in the schema that contains this field
     */
    protected void initialise(VectorSchemaRoot root, int fieldIndex) {
        _fieldIndex = fieldIndex;
    }


    /**
     * Stores the value (held in the entity field accessor) into the arrow row specified
     *
     * @param efa the entity field accessor that contains the value
     * @param row the Arrow row to store the data at
     */
    public abstract void store(EntityFieldAccessor efa, int row);


    /**
     * Loads the value (held in the Arrow row) into the entity field accessor
     *
     * @param efa the entity field accessor to load the value into
     * @param row the Arrow row to get the data from
     */
    public abstract void load(EntityFieldAccessor efa, int row);


    /**
     * This class handles the Arrow schema interaction for byte fields.
     */
    public static final class ByteFieldAccessor extends SchemaFieldAccessor {
        private TinyIntVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant IntVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof TinyIntVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (TinyIntVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getByte());
            }
            else {
                _vector.setNull(row);
            }
        }


        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for byte fields.
     */
    public static final class ShortFieldAccessor extends SchemaFieldAccessor {
        private SmallIntVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant IntVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof SmallIntVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (SmallIntVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getShort());
            }
            else {
                _vector.setNull(row);
            }
        }


        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for int fields.
     */
    public static final class IntFieldAccessor extends SchemaFieldAccessor {
        private IntVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant IntVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof IntVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (IntVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getInt());
            }
            else {
                _vector.setNull(row);
            }
        }


        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for long fields.
     */
    public static final class LongFieldAccessor extends SchemaFieldAccessor {
        private BigIntVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant BigIntVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof BigIntVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (BigIntVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getLong());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Loads the value (held in the Arrow row) into the entity field accessor
         *
         * @param efa the entity field accessor to load the value into
         * @param row the Arrow row to get the data from
         */
        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for float fields.
     */
    public static final class FloatFieldAccessor extends SchemaFieldAccessor {
        private Float4Vector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant Float4Vector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root,fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof Float4Vector)) {
                throw new IllegalArgumentException();
            }

            _vector = (Float4Vector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getFloat());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Loads the value (held in the Arrow row) into the entity field accessor
         *
         * @param efa the entity field accessor to load the value into
         * @param row the Arrow row to get the data from
         */
        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for double fields.
     */
    public static final class DoubleFieldAccessor extends SchemaFieldAccessor {
        private Float8Vector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant Float8Vector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof Float8Vector)) {
                throw new IllegalArgumentException();
            }

            _vector = (Float8Vector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getDouble());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Loads the value (held in the Arrow row) into the entity field accessor
         *
         * @param efa the entity field accessor to load the value into
         * @param row the Arrow row to get the data from
         */
        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for boolean fields.
     */
    public static final class BooleanFieldAccessor extends SchemaFieldAccessor {
        private BitVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant BitVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof BitVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (BitVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getBoolean() ? 1 : 0);
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Loads the value (held in the Arrow row) into the entity field accessor
         *
         * @param efa the entity field accessor to load the value into
         * @param row the Arrow row to get the data from
         */
        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row)!=0);
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * This class handles the Arrow schema interaction for string fields.
     */
    public static final class StringFieldAccessor extends SchemaFieldAccessor {
        private VarCharVector _vector;

        /**
         * Initialises the basic schema details for this field
         * and extracts the relevant VarCharVector from the arrow
         * schema.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        public void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            FieldVector fv = root.getVector(fieldIndex);
            if (fv==null || !(fv instanceof VarCharVector)) {
                throw new IllegalArgumentException();
            }

            _vector = (VarCharVector)fv;
        }


        /**
         * Stores the value (held in the entity field accessor) into the arrow row specified
         *
         * @param efa the entity field accessor that contains the value
         * @param row the Arrow row to store the data at
         */
        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                NullableVarCharHolder tmpVCHolder = RaphtoryStringUtils.getTmpVarCharHolder();

                byte[] tmpBytes = RaphtoryStringUtils.copy(efa.getString(), tmpVCHolder);
                int len = tmpVCHolder.end - tmpVCHolder.start;
                //if (len>512 || len<0) { System.out.println("JS5: " + len); }

                _vector.setSafe(row, tmpBytes, tmpVCHolder.start, tmpVCHolder.end - tmpVCHolder.start);
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Loads the value (held in the Arrow row) into the entity field accessor
         *
         * @param efa the entity field accessor to load the value into
         * @param row the Arrow row to get the data from
         */
        @Override
        public void load(EntityFieldAccessor efa, int row) {
            // TODO: Check the cost of the thread-local used to get tmpVCHolder
            // TODO: Maybe specialize for strings or always have a NullableVarCharHolder
            // TODO: as a parameter?
            NullableVarCharHolder tmpVCHolder = RaphtoryStringUtils.getTmpVarCharHolder();
            _vector.get(row, tmpVCHolder);

            if (tmpVCHolder.isSet!=0) {
                StringBuilder sb = efa.getString();
                sb.setLength(0);
                RaphtoryStringUtils.copy(tmpVCHolder, sb);
            }
            else {
                efa.reset();
            }
        }
    }
}
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
 * This class interacts with VersionedEntityPropertyAccessor to
 * enable user-defined properties to be created and used by entities.
 *<p>
 * The VersionedPropertyStore encapsulates the Arrow specific
 * bits of the access - the schema creation, accessing using row etc.
 *<p>
 * References to the actual arrow vectors are stored here for faster access.
 *v
 * The only state that should be held by instances and sub-classes of this
 * class is the actual Arrow vector, as there will be 1 instance of this
 * for each property in each arrow file and multiple threads may be accessing
 * the same Arrow file concurrently.
 *v
 * The concrete sub-classes of VersionedPropertyStore perform the
 * actual storage and retrieval via Arrow, and also load the value into
 * the appropriate VersionedEntityPropertyAccessor.
 *<p>
 * Properties differ from Fields in Raphtory because a property's value can
 * change over time and the changes are tracked (history). A field is assumed
 * to be set once and doesn't change.
 *<p>
 * <p>TODO: Add support for enum fields
 *
 * @see VersionedEntityPropertyAccessor
 */
public abstract class VersionedPropertyStore {
    protected BigIntVector _localId;
    protected BitVector _initialValue;
    protected BigIntVector _creationTime;
    protected IntVector _prevPtr;


    /**
     * Initialises this object - the individual vectors in the root
     * will be retrieved and stored here for faster access.
     *
     * @param root the Arrow schema root to use
     */
    protected void initialise(VectorSchemaRoot root) {
        if (root!=null) {
            _localId = (BigIntVector) root.getVector(VersionedProperty.LOCAL_ID_FIELD);
            _initialValue = (BitVector) root.getVector(VersionedProperty.INITIAL_VALUE_FIELD);
            _creationTime = (BigIntVector) root.getVector(VersionedProperty.CREATION_TIME_FIELD);
            _prevPtr = (IntVector) root.getVector(VersionedProperty.PREV_PTR_FIELD);
        }
        else {
            _localId = null;
            _initialValue = null;
            _creationTime = null;
            _prevPtr = null;
        }
    }


    /**
     * Stores a property (held in a pojo?) into the Arrow schema
     *
     * @param efa the actual value to store
     * @param row the row to store at
     */
    public abstract void store(VersionedEntityPropertyAccessor efa, int row);


    /**
     * Retrieves a property from the Arrow schema
     *
     * @param efa where to store the property value
     * @param row the row to retrieve from
     */
    public void load(VersionedEntityPropertyAccessor efa, int row) {
        if (row==-1 || _localId.isSet(row)==0) {
            efa.reset();
            return;
        }

        efa.set(_localId.get(row), _initialValue.get(row)!=0, _creationTime.get(row), _prevPtr.get(row));
    }


    /**
     * Int Property Accessor - contains the specific code to process
     * an Arrow IntVector and int-values.
     */
    public static final class IntPropertyAccessor extends VersionedPropertyStore {
        private IntVector _vector;

        /**
         * Retrieves the IntVector to be used for int-property values.
         *
         * @param root the Arrow schema root to use
         */
        @Override
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof IntVector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (IntVector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores an int property in the appropriate IntVector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getInt());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves an int value from the appropriate IntVector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * Long Property Accessor - contains the specific code to process
     * an Arrow BigIntVector and long-values.
     */
    public static final class LongPropertyAccessor extends VersionedPropertyStore {
        private BigIntVector _vector;


        /**
         * Retrieves the BigIntVector to be used for long-property values.
         *
         * @param root the Arrow schema root to use
         */
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof BigIntVector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (BigIntVector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores a long property in the appropriate BigIntVector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getLong());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves a long value from the appropriate BigIntVector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * Float Property Accessor - contains the specific code to process
     * an Arrow Float4Vector and float-values.
     */
    public static final class FloatPropertyAccessor extends VersionedPropertyStore {
        private Float4Vector _vector;

        /**
         * Retrieves the Float4Vector to be used for float-property values.
         *
         * @param root the Arrow schema root to use
         */
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof Float4Vector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (Float4Vector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores a float property in the appropriate Float4Vector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getFloat());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves a float value from the appropriate Float4Vector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * Double Property Accessor - contains the specific code to process
     * an Arrow Float8Vector and double-values.
     */
    public static final class DoublePropertyAccessor extends VersionedPropertyStore {
        private Float8Vector _vector;

        /**
         * Retrieves the Float8Vector to be used for double-property values.
         *
         * @param root the Arrow schema root to use
         */
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof Float8Vector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (Float8Vector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores a double property in the appropriate Float8Vector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getDouble());
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves a double value from the appropriate Float8Vector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row));
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * Boolean/Bit Property Accessor - contains the specific code to process
     * an Arrow BitVector and boolean-values.
     */
    public static final class BooleanPropertyAccessor extends VersionedPropertyStore {
        private BitVector _vector;

        /**
         * Retrieves the BitVector to be used for boolean-property values.
         *
         * @param root the Arrow schema root to use
         */
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof BitVector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (BitVector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores a boolean property in the appropriate BitVector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                _vector.setSafe(row, efa.getBoolean() ? 1 : 0);
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves a boolean value from the appropriate BitVector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

            if (!_vector.isNull(row)) {
                efa.set(_vector.get(row)!=0);
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * String Property Accessor - contains the specific code to process
     * an Arrow VarCharVector and string-values.
     */
    public static final class StringPropertyAccessor extends VersionedPropertyStore {
        private VarCharVector _vector;


        /**
         * Retrieves the VarCharVector to be used for string-property values.
         *
         * @param root the Arrow schema root to use
         */
        public void initialise(VectorSchemaRoot root) {
            super.initialise(root);

            if (root!=null) {
                FieldVector fv = root.getVector(VersionedProperty.FIRST_USER_FIELD);
                if (fv == null || !(fv instanceof VarCharVector)) {
                    throw new IllegalArgumentException();
                }

                _vector = (VarCharVector) fv;
            }
            else {
                _vector = null;
            }
        }


        /**
         * Stores a string property in the appropriate VarCharVector
         *
         * @param efa the actual value to store
         * @param row the row to store at
         */
        @Override
        public void store(VersionedEntityPropertyAccessor efa, int row) {
            if (efa.isSet()) {
                NullableVarCharHolder tmpVCHolder = RaphtoryStringUtils.getTmpVarCharHolder();

                byte[] tmpBytes = RaphtoryStringUtils.copy(efa.getString(), tmpVCHolder);
                /*
                int len = tmpVCHolder.end - tmpVCHolder.start;
                if (len > 512 || len < 0) {
                    System.out.println("JS5: " + len);
                }
                */

                _vector.setSafe(row, tmpBytes, tmpVCHolder.start, tmpVCHolder.end - tmpVCHolder.start);
            }
            else {
                _vector.setNull(row);
            }
        }


        /**
         * Retrieves a string value from the appropriate VarCharVector
         * and stores it in the entity-property-accessor.
         *
         * @param efa where to store the property value
         * @param row the row to retrieve from
         */
        @Override
        public void load(VersionedEntityPropertyAccessor efa, int row) {
            super.load(efa, row);

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
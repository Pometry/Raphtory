/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;


/**
 * This class is used to hold property values for read and write purposes.
 *<p>
 * It works in conjunction with the VersionedPropertyStore which handles
 * the Arrow schema part, leaving this class to provide POJO based storage
 * for the actual values.
 *<p>
 * The concrete sub-classes of VersionedEntityPropertyAccessor implement the
 * actual set, get and load of the property value.
 *<p>
 * Properties differ from Fields in Raphtory because a property's value can
 * change over time and the changes are tracked (history). A field is assumed
 * to be set once and doesn't change.
 *<p>
 * <p>TODO: Add support for enum fields
 *
 * @see VersionedPropertyStore
 */
public abstract class VersionedEntityPropertyAccessor {
    protected boolean _isSet;
    protected long _localId;
    protected boolean _initialValue;
    protected long _creationTime;
    protected int _prevPtr;

    /* This id is the index of the history pointer field
     * (for previous values of this property)
     * in the owning entity's (Vertex or Edge) schema.
     * ie. It tells us which IntVector in the Vertex or Edge
     * Arrow schema contains the head of the linked list
     * for this property.
     */
    protected int _entitySchemaPrevPtrFieldId = -1;


    protected VersionedEntityPropertyAccessor() {}


    /**
     * @return true, if this property has been set, false otherwise
     */
    public boolean isSet() { return _isSet; }


    /**
     * Used internally to set the property back to an
     * unset state.
     */
    public void reset() {
        _isSet = false;
    }


    /**
     * @return the field-id of the prev-ptr field for the history of this property
     */
    protected int getEntityPrevPtrFieldId() {
        return _entitySchemaPrevPtrFieldId;
    }


    /**
     * Sets the field-id of the prev-ptr field for the history of this property
     *
     * @param id the value to set
     */
    protected void setEntityPrevPtrFieldId(int id) {
        _entitySchemaPrevPtrFieldId = id;
    }


    /**
     * Sets the base details for a property
     *
     * @param localId the entity's local-id
     * @param initialValue true if the property was active at that time, false otherwise
     * @param creationTime the associated time
     * @param prevPtr the index of the next item in this list
     */
    protected void set(long localId, boolean initialValue, long creationTime, int prevPtr) {
        _localId = localId;
        _initialValue = initialValue;
        _creationTime = creationTime;
        _prevPtr = prevPtr;
    }


    /**
     * Sets the base details for this property
     *
     * @param initialValue true if the property was active at that time, false otherwise
     * @param creationTime the associated time
     *
     * @return this
     */
    public VersionedEntityPropertyAccessor setHistory(boolean initialValue, long creationTime) {
        _initialValue = initialValue;
        _creationTime = creationTime;
        return this;
    }


    /**
     * @return the local-id of the entity associated with this property
     */
    public long getLocalId() { return _localId; }


    /**
     * @return true if the property was active at the relevant time, false otherwise
     */
    public boolean getInitialValue() { return _initialValue; }


    /**
     * @return the creation time associated with this property
     */
    public long getCreationTime() { return _creationTime; }


    /**
     * @return the row-id of the next item in the history list for this property, or -1 if none
     */
    public int getPrevPtr()  { return _prevPtr; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(int value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(long value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(float value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(double value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(boolean value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(CharSequence value) { load(value); _isSet = true; }


    /**
     * set the actual property value
     *
     * @param value the value to set
     */
    public void set(Object value) { load(value); _isSet = true; }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not int
     */
    public int getInt() { throw new IllegalArgumentException("value is not int"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not long
     */
    public long getLong() { throw new IllegalArgumentException("value is not long"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not float
     */
    public float getFloat() { throw new IllegalArgumentException("value is not float"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not double
     */
    public double getDouble() { throw new IllegalArgumentException("value is not double"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not boolean
     */
    public boolean getBoolean() { throw new IllegalArgumentException("value is not boolean"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not string
     */
    public StringBuilder getString() { throw new IllegalArgumentException("value is not string"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not an object
     * @throws UnsupportedOperationException as this functionality hasn't been completed
     */
    public Object get() { throw new UnsupportedOperationException(); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(int value) { throw new IllegalArgumentException("value is not int"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(long value) { throw new IllegalArgumentException("value is not long"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(float value) { throw new IllegalArgumentException("value is not float"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(double value) { throw new IllegalArgumentException("value is not double"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(boolean value) { throw new IllegalArgumentException("value is not boolean"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(StringBuilder value) { throw new IllegalArgumentException("value is not string"); }

    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(CharSequence value) { throw new IllegalArgumentException("value is not string"); }


    /**
     * Internal method to set the actual property value
     *
     * @param value the property value
     */
    protected void load(Object value) { throw new UnsupportedOperationException(); }


    /**
     * Int Property Accessor - contains the specific code to process int values.
     */
    public static final class IntPropertyAccessor extends VersionedEntityPropertyAccessor {
        private int _value;

        public IntPropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(int value) {
            _value = value;
        }


        /**
         * @return the int property value
         */
        @Override
        public int getInt() {
            return _value;
        }


        /**
         * Clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0;
        }
    }


    /**
     * Long Property Accessor - contains the specific code to process long values.
     */
    public static final class LongPropertyAccessor extends VersionedEntityPropertyAccessor {
        private long _value;

        public LongPropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(long value) {
            _value = value;
        }


        /**
         * @return the long property value
         */
        @Override
        public long getLong() {
            return _value;
        }


        /**
         * clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0L;
        }
    }


    public static final class FloatPropertyAccessor extends VersionedEntityPropertyAccessor {
        private float _value;

        public FloatPropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(float value) {
            _value = value;
        }


        /**
         * @return the float property value
         */
        @Override
        public float getFloat() {
            return _value;
        }


        /**
         * clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0.0f;
        }
    }


    public static final class DoublePropertyAccessor extends VersionedEntityPropertyAccessor {
        private double _value;

        public DoublePropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(double value) {
            _value = value;
        }


        /**
         * @return the double property value
         */
        @Override
        public double getDouble() {
            return _value;
        }


        /**
         * clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0.0d;
        }
    }


    public static final class BooleanPropertyAccessor extends VersionedEntityPropertyAccessor {
        private boolean _value;

        public BooleanPropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(boolean value) {
            _value = value;
        }


        /**
         * @return the boolean property value
         */
        @Override
        public boolean getBoolean() {
            return _value;
        }


        /**
         * clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value = false;
        }
    }


    public static final class StringPropertyAccessor extends VersionedEntityPropertyAccessor {
        private StringBuilder _value = new StringBuilder();

        public StringPropertyAccessor() {}


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(StringBuilder value) {
            _value.setLength(0);
            _value.append(value);
        }


        /**
         * Stores the value into this property accessor
         *
         * @param value the property value
         */
        @Override
        protected void load(CharSequence value) {
            _value.setLength(0);
            _value.append(value);
        }



        /**
         * @return the string property value
         */
        @Override
        public StringBuilder getString() {
            return _value;
        }


        /**
         * clear this property
         */
        @Override
        public void reset() {
            super.reset();
            _value.setLength(0);
        }
    }
}
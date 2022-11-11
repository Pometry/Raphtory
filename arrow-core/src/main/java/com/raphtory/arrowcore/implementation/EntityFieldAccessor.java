/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;


/**
 * This class is used to hold field values for read and write purposes.
 *<p>
 * It works in conjunction with the SchemaFieldAccessor which handles
 * the Arrow schema part, leaving this class to provide POJO based storage
 * for the actual values.
 *<p>
 * The concrete sub-classes of EntityFieldAccessor implement the
 * actual set, get and load of the field value.
 *<p>
 * Properties differ from Fields in Raphtory because a property's value can
 * change over time and the changes are tracked (history). A field is assumed
 * to be set once and doesn't change.
 *<p>
 * <p>TODO: Add support for enum fields
 *
 * @see SchemaFieldAccessor
 */
public abstract class EntityFieldAccessor {
    protected boolean _isSet;

    protected EntityFieldAccessor() {}


    /**
     * @return true if the value has been set, false otherwise
     */
    public boolean isSet() { return _isSet; }


    /**
     * Resets this instance
     */
    public void reset() {
        _isSet = false;
    }


    /**
     * Sets the byte value of this field
     *
     * @param value the value in question
     */
    public void set(byte value) { throw new IllegalArgumentException("value is not byte"); }


    /**
     * Sets the short value of this field
     *
     * @param value the value in question
     */
    public void set(short value) { throw new IllegalArgumentException("value is not short"); }


    /**
     * Sets the int value of this field
     *
     * @param value the value in question
     */
    public void set(int value) { throw new IllegalArgumentException("value is not int"); }


    /**
     * Sets the long value of this field
     *
     * @param value the value in question
     */
    public void set(long value) { throw new IllegalArgumentException("value is not long"); }


    /**
     * Sets the float value of this field
     *
     * @param value the value in question
     */
    public void set(float value) { throw new IllegalArgumentException("value is not float"); }


    /**
     * Sets the double value of this field
     *
     * @param value the value in question
     */
    public void set(double value) { throw new IllegalArgumentException("value is not double"); }


    /**
     * Sets the boolean value of this field
     *
     * @param value the value in question
     */
    public void set(boolean value) { throw new IllegalArgumentException("value is not boolean"); }


    /**
     * Sets the string value of this field
     *
     * @param value the value in question
     */
    public void set(CharSequence value) { throw new IllegalArgumentException("value is not string"); }


    /**
     * Stores the value into this field accessor
     *
     * @param ordinal the ordinal value of the enum
     */
    public void setEnum(int ordinal) { throw new IllegalArgumentException("value is not enum"); }


    /**
     * Stores the value into this field accessor
     *
     * @param value the enum value
     */
    public void setEnum(Enum<?> value) { throw new IllegalArgumentException("value is not enum"); }


    /**
         * Sets the object value of this field (still a work in progress)
         *
         * @param value the value in question
         */
    public void set(Object value) { throw new UnsupportedOperationException(); }


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
     * @throws IllegalArgumentException if not enum
     */
    public Enum<?> getEnum() { throw new IllegalArgumentException("value is not enum"); }

    /**
     * Still a work in progress ;-)
     *
     * @return the property value
     * @throws IllegalArgumentException if not object
     */
    public Object get() { throw new UnsupportedOperationException(); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not byte
     */
    public byte getByte() { throw new IllegalArgumentException("value is not byte"); }


    /**
     * @return the property value
     * @throws IllegalArgumentException if not short
     */
    public short getShort() { throw new IllegalArgumentException("value is not short"); }


    /**
     * Byte Property Accessor - contains the specific code to process byte values.
     */
    public static final class ByteFieldAccessor extends EntityFieldAccessor {
        private byte _value;

        public ByteFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(byte value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the byte field value
         */
        @Override
        public byte getByte() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0;
        }
    }


    /**
     * Short Property Accessor - contains the specific code to process short values.
     */
    public static final class ShortFieldAccessor extends EntityFieldAccessor {
        private short _value;

        public ShortFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(short value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the short field value
         */
        @Override
        public short getShort() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0;
        }
    }


    /**
     * Int Property Accessor - contains the specific code to process int values.
     */
    public static final class IntFieldAccessor extends EntityFieldAccessor {
        private int _value;

        public IntFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(int value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the int field value
         */
        @Override
        public int getInt() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0;
        }
    }


    public static final class LongFieldAccessor extends EntityFieldAccessor {
        private long _value;

        public LongFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(long value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the long field value
         */
        @Override
        public long getLong() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0L;
        }
    }


    /**
     * Float Property Accessor - contains the specific code to process float values.
     */
    public static final class FloatFieldAccessor extends EntityFieldAccessor {
        private float _value;

        public FloatFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(float value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the float field value
         */
        @Override
        public float getFloat() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0.0f;
        }
    }


    /**
     * Double Property Accessor - contains the specific code to process double values.
     */
    public static final class DoubleFieldAccessor extends EntityFieldAccessor {
        private double _value;

        public DoubleFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(double value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the double field value
         */
        @Override
        public double getDouble() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = 0.0d;
        }
    }


    /**
     * Boolean Property Accessor - contains the specific code to process boolean values.
     */
    public static final class BooleanFieldAccessor extends EntityFieldAccessor {
        private boolean _value;

        public BooleanFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(boolean value) {
            _value = value;
            _isSet = true;
        }


        /**
         * @return the boolean field value
         */
        @Override
        public boolean getBoolean() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value = false;
        }
    }


    /**
     * String Property Accessor - contains the specific code to process string values.
     */
    public static final class StringFieldAccessor extends EntityFieldAccessor {
        private StringBuilder _value = new StringBuilder();

        public StringFieldAccessor() {}


        /**
         * Stores the value into this field accessor
         *
         * @param value the field value
         */
        @Override
        public void set(CharSequence value) {
            _value.setLength(0);
            _value.append(value);
            _isSet = true;
        }


        /**
         * @return the string field value
         */
        @Override
        public StringBuilder getString() {
            return _value;
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _value.setLength(0);
        }
    }
}
/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

/**
 * NonversionedField - models a field consisting of a type and a value.
 * Changes to a field are not recorded.
 *<p>
 * A NonversionedField handles the basic primitive types and strings.
 *<p>
 * This class works in conjunction with SchemaFieldAccessor (which handles the field within
 * an Arrow Schemea) and EntityFieldAccessor (which handles the field within a POJO)
 *
 * @see SchemaFieldAccessor
 * @see EntityFieldAccessor
 *
 */
public class NonversionedEnumField extends NonversionedField {
    protected final int _maxOrdinal;
    protected final Enum<?>[] _enumValues;

    /**
     * Creates a new field
     *
     * @param name the name of the field
     * @param c the type of the field value
     */
    public  <T extends Enum<T>> NonversionedEnumField(String name, Class<T> c) {
        super(name, c);

        Enum<?>[] values = c.getEnumConstants();
        int maxOrdinal = 0;
        for (Enum<?> it: values) {
            if (it.ordinal()>maxOrdinal) {
                maxOrdinal = it.ordinal();
            }
        }

        _maxOrdinal = maxOrdinal;
        _enumValues = new Enum<?>[_maxOrdinal+1];
        for (Enum<?> it: values) {
            _enumValues[it.ordinal()] = it;
        }
    }


    /**
     * Adds this field to the Arrow schema
     *
     * @param schema the schema to which the field will be added
     */
    public void addToSchema(List<Field> schema) {
        Field f;
        if (_maxOrdinal<=255) {
            f = new Field(_name, new FieldType(false, _typeMap.get(byte.class).get(), null), null);
        }
        else if (_maxOrdinal<=65535) {
            f = new Field(_name, new FieldType(false, _typeMap.get(short.class).get(), null), null);
        }
        else {
            f = new Field(_name, new FieldType(false, _typeMap.get(int.class).get(), null), null);
        }
        schema.add(f);
    }


    /**
     * Returns a new schema-field accessor configured for this field
     *
     * @param root the Arrow vector schema root to us
     *
     * @return the new schema-field-accessor
     */
    public SchemaFieldAccessor getNewSchemaFieldAccessor(VectorSchemaRoot root) {
        EnumSchemaFieldAccessor fa = new EnumSchemaFieldAccessor();
        fa.initialise(_maxOrdinal);
        fa.initialise(root, _parentSchemaFieldIndex);
        return fa;
    }


    /**
     * Returns a new entity-field accessor configured for this field
     *
     * @return the new accessor
     */
    public EntityFieldAccessor getNewEntityFieldAccessor() {
        EnumEntityFieldAccessor fa = new EnumEntityFieldAccessor(this);
        return fa;
    }


    private static class EnumSchemaFieldAccessor extends SchemaFieldAccessor {
        private int _maxOrdinal;
        private TinyIntVector _byteVector;
        private SmallIntVector _shortVector;
        private IntVector _intVector;
        private BaseFixedWidthVector _vector;


        /**
         * Initialises the enum info for this field accessor
         *
         * @param maxOrdinal the max ordinal value for this enum
         */
        protected void initialise(int maxOrdinal) {
            _maxOrdinal = maxOrdinal;
        }


        /**
         * Initialises the basic schema details for this field.
         *
         * @param root the Arrow schema to retrieve fields from
         * @param fieldIndex the field-id in the schema that contains this field
         */
        @Override
        protected void initialise(VectorSchemaRoot root, int fieldIndex) {
            super.initialise(root, fieldIndex);

            _byteVector = null;
            _shortVector = null;
            _intVector = null;
            _vector = null;

            FieldVector fv = root.getVector(fieldIndex);
            if (_maxOrdinal<=255 && fv instanceof TinyIntVector) {
                _byteVector = (TinyIntVector)fv;
                _vector = _byteVector;
            }
            else if (_maxOrdinal<=65535 && fv instanceof SmallIntVector) {
                _shortVector = (SmallIntVector)fv;
                _vector = _shortVector;
            }
            else if (fv instanceof IntVector) {
                _intVector = (IntVector)fv;
                _vector = _intVector;
            }
            else {
                throw new IllegalArgumentException();
            }
        }


        @Override
        public void store(EntityFieldAccessor efa, int row) {
            if (efa.isSet()) {
                EnumEntityFieldAccessor eefa = (EnumEntityFieldAccessor)efa;

                if (_maxOrdinal<=255) {
                    _byteVector.setSafe(row, eefa.getEnum().ordinal());
                }
                else if (_maxOrdinal<=65535) {
                    _shortVector.setSafe(row, eefa.getEnum().ordinal());
                }
                else {
                    _intVector.setSafe(row, eefa.getEnum().ordinal());
                }
            }
            else {
                _vector.setNull(row);
            }
        }


        @Override
        public void load(EntityFieldAccessor efa, int row) {
            if (!_vector.isNull(row)) {
                EnumEntityFieldAccessor eefa = (EnumEntityFieldAccessor)efa;

                if (_maxOrdinal<=255) {
                    eefa.setEnum(_byteVector.get(row) & 0xff);
                }
                else if (_maxOrdinal<=65535) {
                    eefa.setEnum(_shortVector.get(row) & 0xffff);
                }
                else {
                    eefa.setEnum(_intVector.get(row));
                }
            }
            else {
                efa.reset();
            }
        }
    }


    /**
     * Enum Property Accessor - contains the specific code to process int values.
     */
    public static final class EnumEntityFieldAccessor extends EntityFieldAccessor {
        private int _ordinal;
        private final NonversionedEnumField _field;

        public EnumEntityFieldAccessor(NonversionedEnumField field) {
            _field = field;
        }


        /**
         * Stores the value into this field accessor
         *
         * @param ordinal the ordinal value of the enum
         */
        @Override
        public void setEnum(int ordinal) {
            _ordinal = ordinal;
            _isSet = true;
        }


        /**
         * Stores the value into this field accessor
         *
         * @param enumValue the enum value to use
         */
        @Override
        public void setEnum(Enum<?> enumValue) {
            if (enumValue!=null) {
                if (enumValue.getClass() != _field._class) {
                    throw new IllegalArgumentException("wrong enum");
                }

                _ordinal = enumValue.ordinal();
                _isSet = true;
            }
            else {
                reset();
            }
        }


        /**
         * @return the enum field value
         */
        @Override
        public Enum<?> getEnum() {
            if (_ordinal>=0 && _ordinal<=_field._maxOrdinal) {
                return _field._enumValues[_ordinal];
            }
            else {
                return null;
            }
        }


        /**
         * Clear this field
         */
        @Override
        public void reset() {
            super.reset();
            _ordinal = -1;
        }
    }
}
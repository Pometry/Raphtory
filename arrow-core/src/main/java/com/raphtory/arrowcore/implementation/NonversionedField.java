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
 * <p>TODO: Add support for more complex values (eg. a POJO object representing a list)
 * <p>TODO: by adding a type-registry and appropriate mappings.
 */
public class NonversionedField {
    public static final HashMap<Class, Supplier<ArrowType>> _typeMap = new HashMap<>();
    public static final HashMap<Class, Supplier<SchemaFieldAccessor>> _schemaAccessorMap = new HashMap<>();
    public static final HashMap<Class, Supplier<EntityFieldAccessor>> _entityAccessorMap = new HashMap<>();


    /**
     * Initialises the default mappings for value types, accessors for controlling Arrow vectors
     * and accessors for controlling POJO values.
     */
    static {
        _typeMap.put(byte.class,    () -> new ArrowType.Int(8, true));
        _typeMap.put(short.class,   () -> new ArrowType.Int(16, true));
        _typeMap.put(int.class,     () -> new ArrowType.Int(32, true));
        _typeMap.put(long.class,    () -> new ArrowType.Int(64, true));
        _typeMap.put(float.class,   () -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        _typeMap.put(double.class,  () -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        _typeMap.put(boolean.class,       ArrowType.Bool::new);
        _typeMap.put(StringBuilder.class, ArrowType.Utf8::new);

        _schemaAccessorMap.put(byte.class,    SchemaFieldAccessor.ByteFieldAccessor::new);
        _schemaAccessorMap.put(short.class,   SchemaFieldAccessor.ShortFieldAccessor::new);
        _schemaAccessorMap.put(int.class,     SchemaFieldAccessor.IntFieldAccessor::new);
        _schemaAccessorMap.put(long.class,    SchemaFieldAccessor.LongFieldAccessor::new);
        _schemaAccessorMap.put(float.class,   SchemaFieldAccessor.FloatFieldAccessor::new);
        _schemaAccessorMap.put(double.class,  SchemaFieldAccessor.DoubleFieldAccessor::new);
        _schemaAccessorMap.put(boolean.class, SchemaFieldAccessor.BooleanFieldAccessor::new);
        _schemaAccessorMap.put(StringBuilder.class, SchemaFieldAccessor.StringFieldAccessor::new);

        _entityAccessorMap.put(byte.class,    EntityFieldAccessor.ByteFieldAccessor::new);
        _entityAccessorMap.put(short.class,   EntityFieldAccessor.ShortFieldAccessor::new);
        _entityAccessorMap.put(int.class,     EntityFieldAccessor.IntFieldAccessor::new);
        _entityAccessorMap.put(long.class,    EntityFieldAccessor.LongFieldAccessor::new);
        _entityAccessorMap.put(float.class,   EntityFieldAccessor.FloatFieldAccessor::new);
        _entityAccessorMap.put(double.class,  EntityFieldAccessor.DoubleFieldAccessor::new);
        _entityAccessorMap.put(boolean.class, EntityFieldAccessor.BooleanFieldAccessor::new);
        _entityAccessorMap.put(StringBuilder.class, EntityFieldAccessor.StringFieldAccessor::new);
    }



    protected final String _name;
    protected final Class _class;
    protected final Supplier<EntityFieldAccessor> _faSupplier;
    protected int _parentSchemaFieldIndex;


    /**
     * Creates a new field
     *
     * @param name the name of the field
     * @param c the type of the field value
     */
    public NonversionedField(String name, Class c) {
        _name = name.toLowerCase();
        _class = c;
        _faSupplier = _entityAccessorMap.get(_class);
    }


    /**
     * Sets the index of this field in the parent schema
     *
     * @param fi the field index to set
     */
    protected void setParentSchemaFieldIndex(int fi) {
        _parentSchemaFieldIndex = fi;
    }


    /**
     * @return the name of this field
     */
    public String name() {
        return _name;
    }


    /**
     * Adds this field to the Arrow schema
     *
     * @param schema the schema to which the field will be added
     */
    public void addToSchema(List<Field> schema) {
        Field f = new Field(_name, new FieldType(false, _typeMap.get(_class).get(), null), null);
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
        SchemaFieldAccessor fa = _schemaAccessorMap.get(_class).get();
        fa.initialise(root, _parentSchemaFieldIndex);
        return fa;
    }


    /**
     * Returns a new entity-field accessor configured for this field
     *
     * @return the new accessor
     */
    public EntityFieldAccessor getNewEntityFieldAccessor() {
        EntityFieldAccessor fa = _faSupplier.get();
        return fa;
    }
}
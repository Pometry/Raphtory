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
 * VersionedProperty - models a property consisting of a type,
 * a value and associated history.
 * <p>
 * An VersionedProperty handles the basic primitive types and strings.
 * <p>
 * This class works in conjunction with VersionedEntityPropertyAccessor (which
 * handles the property within a POJO) and VersionedPropertyStore (which
 * handles the property within an Arrow Schema).
 *
 * @see VersionedEntityPropertyAccessor
 * @see VersionedPropertyStore
 *
 * <p>TODO: Add support for more complex values (eg. a POJO object representing a list)
 * <p>TODO: by adding a type-registry and appropriate mappings.
 */
public class VersionedProperty {
    public static final int LOCAL_ID_FIELD = 0;
    public static final int INITIAL_VALUE_FIELD = 1;
    public static final int CREATION_TIME_FIELD = 2;
    public static final int PREV_PTR_FIELD = 3;

    public static final int N_CORE_FIELDS = 4;
    public static final int FIRST_USER_FIELD = N_CORE_FIELDS;


    public static final HashMap<Class, Supplier<ArrowType>> _typeMap = new HashMap<>();
    public static final HashMap<Class, Supplier<VersionedPropertyStore>> _schemaAccessorMap = new HashMap<>();
    public static final HashMap<Class, Supplier<VersionedEntityPropertyAccessor>> _entityAccessorMap = new HashMap<>();


    /**
     * Initialises the default mappings for value types, accessors for controlling Arrow vectors
     * and accessors for controlling POJO values.
     */
    static {
        _typeMap.put(int.class, () -> new ArrowType.Int(32, true));
        _typeMap.put(long.class, () -> new ArrowType.Int(64, true));
        _typeMap.put(java.lang.Long.class, () -> new ArrowType.Int(64, true));
        _typeMap.put(float.class, () -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        _typeMap.put(double.class, () -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        _typeMap.put(boolean.class, ArrowType.Bool::new);
        _typeMap.put(StringBuilder.class, ArrowType.Utf8::new);

        _schemaAccessorMap.put(int.class, VersionedPropertyStore.IntPropertyAccessor::new);
        _schemaAccessorMap.put(long.class, VersionedPropertyStore.LongPropertyAccessor::new);
        _schemaAccessorMap.put(java.lang.Long.class, VersionedPropertyStore.LongPropertyAccessor::new);
        _schemaAccessorMap.put(float.class, VersionedPropertyStore.FloatPropertyAccessor::new);
        _schemaAccessorMap.put(double.class, VersionedPropertyStore.DoublePropertyAccessor::new);
        _schemaAccessorMap.put(boolean.class, VersionedPropertyStore.BooleanPropertyAccessor::new);
        _schemaAccessorMap.put(StringBuilder.class, VersionedPropertyStore.StringPropertyAccessor::new);

        _entityAccessorMap.put(int.class, VersionedEntityPropertyAccessor.IntPropertyAccessor::new);
        _entityAccessorMap.put(long.class, VersionedEntityPropertyAccessor.LongPropertyAccessor::new);
        _entityAccessorMap.put(java.lang.Long.class, VersionedEntityPropertyAccessor.LongPropertyAccessor::new);
        _entityAccessorMap.put(float.class, VersionedEntityPropertyAccessor.FloatPropertyAccessor::new);
        _entityAccessorMap.put(double.class, VersionedEntityPropertyAccessor.DoublePropertyAccessor::new);
        _entityAccessorMap.put(boolean.class, VersionedEntityPropertyAccessor.BooleanPropertyAccessor::new);
        _entityAccessorMap.put(StringBuilder.class, VersionedEntityPropertyAccessor.StringPropertyAccessor::new);
    }


    protected final String _name;
    protected final Class _class;
    protected final Supplier<VersionedEntityPropertyAccessor> _accessor;
    protected int _prevPtrFieldIndexInEntitySchema;


    /**
     * Creates a new property
     *
     * @param name the name of the property
     * @param c    the type of the property value
     */
    public VersionedProperty(String name, Class c) {
        _name = name.toLowerCase();
        _class = c;
        _accessor = _entityAccessorMap.get(_class);
    }


    /**
     * Sets the index of the prev-ptr vector in the owning entity's schema
     *
     * @param fi the field index
     */
    protected void setPrevPtrFieldIndexInEntity(int fi) {
        _prevPtrFieldIndexInEntitySchema = fi;
    }


    /**
     * @return the name of this parameter
     */
    public String name() {
        return _name;
    }


    /**
     * Adds this property to a schema. The schema will already contain
     * the base fields required for history.
     *
     * @param fields the list to add this property's schema fields to
     */
    protected void addToSchema(List<Field> fields) {
        Field f = new Field(_name, new FieldType(false, _typeMap.get(_class).get(), null), null);
        fields.add(f);
    }


    /**
     * Returns a new schema accessor to access this property from an Arrow schema
     *
     * @param root the vector schema root to use
     * @return a new schema accessor
     */
    public VersionedPropertyStore getNewSchemaFieldAccessor(VectorSchemaRoot root) {
        VersionedPropertyStore fa = _schemaAccessorMap.get(_class).get();
        fa.initialise(root);
        return fa;
    }


    /**
     * Returns a new entity accessor to access this property in a POJO
     *
     * @param fieldId the id of the property
     * @return a new entity accessor
     */
    public VersionedEntityPropertyAccessor getNewEntityFieldAccessor(int fieldId) {
        VersionedEntityPropertyAccessor fa = _accessor.get();
        fa.setEntityPrevPtrFieldId(fieldId);
        return fa;
    }
}
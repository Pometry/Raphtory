/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.model;

import com.raphtory.arrowcore.implementation.EntityFieldAccessor;
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition;
import com.raphtory.arrowcore.implementation.VersionedEntityPropertyAccessor;

/**
 * Experimental base class representing an entity.
 *<p>
 * <p>TODO: This class will be removed as most use cases are available via
 * iterators or via direct calls into the EdgePartitionManager and
 * as such this class contains a number of unused methods.
 */
public abstract class Entity {
    protected long _globalId;
    protected long _localId;
    protected boolean _initialValue;
    protected long _creationTime = -1L;
    protected long _oldestPoint;
    protected int _historyPtr = -1;
    protected final RaphtoryArrowPartition _rap;
    protected final EntityFieldAccessor[] _fieldAccessors;
    protected final VersionedEntityPropertyAccessor[] _propertyAccessors;

    protected volatile int _refCount = 0;


    /**
     * @param rap the Raphtory arrow partition touse
     * @param fieldAccessors the entity field accessors to use
     * @param propertyAccessors the entity property accessors to use
     */
    public Entity(RaphtoryArrowPartition rap, EntityFieldAccessor[] fieldAccessors, VersionedEntityPropertyAccessor[] propertyAccessors) {
        _rap = rap;
        _fieldAccessors = fieldAccessors;
        _propertyAccessors = propertyAccessors;
    }


    /**
     * Resets the base information for this entity
     *
     * @param localId the local id of this entity (vertex or edge)
     * @param globalId the global id of this entity (vertex only)
     * @param initialValue true if the entity is active at this timepoint, false otherwise
     * @param time the associated point in time
     */
    public void init(long localId, long globalId, boolean initialValue, long time) {
        _localId = localId;
        _globalId = globalId;
        _initialValue = initialValue;
        _oldestPoint = time;
        if (_initialValue) {
            _creationTime = time;
        }
    }


    /**
     * Clears the field/property accessors for recycling this entity
     */
    public void reset() {
        if (_fieldAccessors!=null) {
            for (int i=0; i<_fieldAccessors.length; ++i) {
                _fieldAccessors[i].reset();
            }
        }

        if (_propertyAccessors!=null) {
            for (int i=0; i<_propertyAccessors.length; ++i) {
                _propertyAccessors[i].reset();
            }
        }
    }


    /**
     * @return the Raphtory arrow partition for this vertex.
     */
    public RaphtoryArrowPartition getRaphtory() { return _rap; }


    /**
     * @return the current reference count for this entity
     */
    public int getRefCount() {
        return _refCount;
    }


    /**
     * Increments the reference count.
     */
    public void incRefCount() {
        ++_refCount;
    }


    /**
     * Decrements the reference count.
     * If it goes to zero, the entity
     * should take care of recycling itself.
     */
    public abstract void decRefCount();


    /**
     * Resets the history pointer linked-list
     *
     * @param historyPtr the new value to use
     */
    public void resetHistoryData(int historyPtr) {
        _historyPtr = historyPtr;
    }


    /**
     * @return the local-id of this entity
     */
    public long getLocalId() {
        return _localId;
    }


    /**
     * @return the global-id of this entity
     */
    public long getGlobalId() {
        return _globalId;
    }


    /**
     * @return the head of the history linked-list
     */
    public int getHistoryPtr() { return _historyPtr; }


    /**
     * @return true if the entity is active at this time, false otherwise
     */
    public boolean getInitialValue() {
        return _initialValue;
    }


    /**
     * @return the entity's creation time
     */
    public long getCreationTime() {
        return _creationTime;
    }


    /**
     * @return the oldest time point for this entity
     */
    public long getOldestPoint() {
        return _oldestPoint;
    }


    /**
     * Clears this entity for recycling
     */
    public void clear() {
        _globalId = 0;
        _localId = 0;
        _initialValue = false;
        _creationTime = -1L;
        _oldestPoint = -1L;
        _historyPtr = -1;
    }


    /**
     * Recycles this object (can be used to recycle fields as required)
     */
    public void recycle() {
    }


    /**
     * Resets the basic entity details for this entity
     *
     * @param localId this entity's local id
     * @param globalId this entity's global id (for vertices)
     * @param initialValue true if the entity is active at this time
     * @param time the associated point in time
     */
    public void reset(long localId, long globalId, boolean initialValue, long time) {
        this._localId = localId;
        this._globalId = globalId;
        this._initialValue = initialValue;
        if (this._initialValue) {
            _creationTime = time;
        } else {
            _creationTime = -1L;
        }
    }


    /**
     * Makes a clone of this entity
     *
     * @param newOne the object that this entity should be cloned into
     */
    protected void makeClone(Entity newOne) {
        newOne._globalId = _globalId;
        newOne._localId = _localId;
        newOne._initialValue = _initialValue;
        newOne._creationTime = _creationTime;
        newOne._oldestPoint = _oldestPoint;
        newOne._historyPtr = _historyPtr;
    }


    /**
     * Returns the specified field - can be used for getting and
     * setting the field value.
     *
     * @param i the field index in question
     * @return the appropriate entity-field-accessor
     */
    public EntityFieldAccessor getField(int i) {
        return _fieldAccessors[i];
    }


    /**
     * Returns the specified property - can be used for getting and
     * setting the property's value and history.
     *
     * @param i the property index in question
     * @return the appropriate versioned-entity-field-accessor
     */
    public VersionedEntityPropertyAccessor getProperty(int i) {
        return _propertyAccessors[i];
    }
}
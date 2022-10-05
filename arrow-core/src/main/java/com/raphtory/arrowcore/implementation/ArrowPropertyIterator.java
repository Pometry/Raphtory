/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import java.util.Iterator;

/**
 * ArrowPropertyIterator - base class for iterating over properties
 */
public abstract class ArrowPropertyIterator implements Iterator<VersionedEntityPropertyAccessor> {
    /**
     * Initialises this instance
     *
     * @param store the property store to use
     * @param sfa the schema property accessor to use
     * @param efa the entity property accessor to use
     * @param startRow the head of the linked list of properties
     */
    protected abstract void init(PropertyStore store, VersionedPropertyStore sfa, VersionedEntityPropertyAccessor efa, int startRow);


    /**
     * @return true if there are further property records, false otherwise
     */
    public abstract boolean hasNext();


    /**
     * @return the next property in this iterator
     */
    public abstract VersionedEntityPropertyAccessor next();


    /**
     * Property Iterator that iterates over all historical records for a
     * single entity and a single property
     */
    public static class AllPropertiesIterator extends ArrowPropertyIterator {
        protected PropertyStore _store;
        protected VersionedEntityPropertyAccessor _vefa = null;
        protected VersionedPropertyStore _vsfa = null;
        protected int _row = -1;


        /**
         * Initialises this instance
         *
         * @param store the property store to use
         * @param sfa the schema property accessor to use
         * @param efa the entity property accessor to use
         * @param startRow the head of the linked list of properties
         */
        protected void init(PropertyStore store, VersionedPropertyStore sfa, VersionedEntityPropertyAccessor efa, int startRow) {
            _store = store;
            _vsfa = sfa;
            _vefa = efa;
            _row = startRow;
        }


        /**
         * @return true if there are further property records, false otherwise
         */
        @Override
        public boolean hasNext() {
            if (_row==-1) {
                _vefa.reset();
            }

            return _row!=-1;
        }


        /**
         * @return the next property in this iterator
         */
        @Override
        public VersionedEntityPropertyAccessor next() {
            _store.loadProperty(_row, _vefa);
            _row = _vefa._prevPtr;
            return _vefa;
        }
    }
}
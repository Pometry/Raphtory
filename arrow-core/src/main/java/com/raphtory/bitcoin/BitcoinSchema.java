/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.PropertySchema;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class defines the vertex and edge schema used for
 * the sample bitcoin data-set.
 */
public class BitcoinSchema implements PropertySchema {
    private static final ArrayList<NonversionedField> _nonVersionedVertexProperties;
    private static final ArrayList<VersionedProperty>    _versionedVertexProperties;

    private static final ArrayList<NonversionedField> _nonVersionedEdgeProperties;
    private static final ArrayList<VersionedProperty>    _versionedEdgeProperties;

    static {
        _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                new NonversionedField("transaction_hash", StringBuilder.class),
                new NonversionedField("address_chain",    StringBuilder.class),
                new NonversionedField("is_halt",          boolean.class)
        ));

        _nonVersionedEdgeProperties = new ArrayList<>(Arrays.asList(
                new NonversionedField("amount",      long.class),
                new NonversionedField("is_received", boolean.class)
        ));

        //_versionedVertexProperties = null;
        //_versionedEdgeProperties   = null;

        /* */
        _versionedVertexProperties = new ArrayList<>(Arrays.asList(
                // Dummy field used for testing
                new VersionedProperty("magic_vertex", long.class)
        ));

        _versionedEdgeProperties = new ArrayList<>(Arrays.asList(
                // Dummy field used for testing
                new VersionedProperty("magic_edge", long.class)
        ));
        /* */
    }


    @Override
    public ArrayList<NonversionedField> nonversionedVertexProperties() {
        return _nonVersionedVertexProperties;
    }


    @Override
    public ArrayList<VersionedProperty> versionedVertexProperties() {
        return _versionedVertexProperties;
    }


    @Override
    public ArrayList<NonversionedField> nonversionedEdgeProperties() {
        return _nonVersionedEdgeProperties;
    }


    @Override
    public ArrayList<VersionedProperty> versionedEdgeProperties() {
        return _versionedEdgeProperties;
    }
}
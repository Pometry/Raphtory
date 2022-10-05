/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore;

import com.raphtory.arrowcore.implementation.NonversionedField;
import com.raphtory.arrowcore.implementation.VersionedProperty;
import com.raphtory.arrowcore.model.PropertySchema;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class defines the vertex and edge schema used for
 * the simple tests.
 */
public class SimpleTestSchema implements PropertySchema {
    private static final ArrayList<NonversionedField> _nonVersionedVertexProperties;
    private static final ArrayList<VersionedProperty>    _versionedVertexProperties;

    private static final ArrayList<NonversionedField> _nonVersionedEdgeProperties;
    private static final ArrayList<VersionedProperty>    _versionedEdgeProperties;

    static {
        _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                new NonversionedField("name", StringBuilder.class)
        ));

        _nonVersionedEdgeProperties = null;
        _versionedVertexProperties = null;
        _versionedEdgeProperties   = null;
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
/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.NonversionedEnumField;
import com.raphtory.arrowcore.implementation.NonversionedField;
import com.raphtory.arrowcore.implementation.VersionedProperty;
import com.raphtory.arrowcore.model.PropertySchema;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class defines the vertex and edge schema used for
 * the sample Google+ data-set.
 *
 * Specifically, there are no additional fields or properties.
 */
public class GPSchema implements PropertySchema {
    private static ArrayList<NonversionedField> _nonVersionedVertexProperties = new ArrayList<>();
    private static ArrayList EMPTY = new ArrayList();

    public static enum TEST_ENUM {
        A, B, C
    };

    static {
        _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                new NonversionedField("userid", StringBuilder.class),
                new NonversionedEnumField("xxx", TEST_ENUM.class)
        ));
    }


    @Override
    public ArrayList<NonversionedField> nonversionedVertexProperties() {
        return _nonVersionedVertexProperties;
    }


    @Override
    public ArrayList<VersionedProperty> versionedVertexProperties() {
        return EMPTY;
    }


    @Override
    public ArrayList<NonversionedField> nonversionedEdgeProperties() {
        return EMPTY;
    }


    @Override
    public ArrayList<VersionedProperty> versionedEdgeProperties() {
        return EMPTY;
    }
}
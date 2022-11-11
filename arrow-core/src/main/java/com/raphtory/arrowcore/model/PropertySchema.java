/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.model;

import com.raphtory.arrowcore.implementation.NonversionedField;
import com.raphtory.arrowcore.implementation.VersionedProperty;

import java.util.ArrayList;

/**
 * A client implements this interface and supplies it to the RaphtoryArrowPartition,
 * in order to describe the properties and fields that are to be stored and processed.
 *<p>
 * <p>TODO: Remove the references to the implementation classes and use Property and Field instead.
 * <p>TODO: Provide a registry for more complex object values and their appropriate scheme and entity
 * <p>TODO: counterparts.
 */
public interface PropertySchema {
    /**
     * @return a list of fields that are to be stored/processed in each vertex
     */
    ArrayList<NonversionedField> nonversionedVertexProperties();


    /**
     * @return a list of properties that are to stored/processed in each vertex
     */
    ArrayList<VersionedProperty> versionedVertexProperties();


    /**
     * @return a list of fields that are to be stored/processed in each edge
     */
    ArrayList<NonversionedField> nonversionedEdgeProperties();


    /**
     * @return a list of properties that are to stored/processed in each edge
     */
    ArrayList<VersionedProperty> versionedEdgeProperties();
}
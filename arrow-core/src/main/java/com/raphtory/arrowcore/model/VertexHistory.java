/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */
package com.raphtory.arrowcore.model;

public class VertexHistory {
    //         historyFields.add(new Field("vertex_row", new FieldType(false, new ArrowType.Int(32, true), null), null));
    //        historyFields.add(new Field("time", new FieldType(false, new ArrowType.Int(64, true), null), null));
    //        historyFields.add(new Field("active", new FieldType(false, new ArrowType.Bool(), null), null));
    //        historyFields.add(new Field("update", new FieldType(false, new ArrowType.Bool(), null), null));
    //        historyFields.add(new Field("prev_ptr", new FieldType(false, new ArrowType.Int(32, true), null), null));
    //        historyFields.add(new Field("sorted_time_index", new FieldType(false, new ArrowType.Int(32, true), null), null));
    //
    //        // These will contain edge-ids if this history record is related to an edge
    //        historyFields.add(new Field("edge_id", new FieldType(true, new ArrowType.Int(64, true), null), null));
    //        historyFields.add(new Field("is_outgoing", new FieldType(false, new ArrowType.Bool(), null), null));

    protected long _vertexId;
    protected long _time;
    protected boolean _isActive;
    protected boolean _wasUpdated;
    protected long _edgeId;
    protected boolean _isOutgoingEdge;
}

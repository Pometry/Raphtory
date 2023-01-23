package com.raphtory.internals.graph

import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.components.querymanager._
import com.raphtory.protocol

sealed private[raphtory] trait GraphAlteration

private[raphtory] object GraphAlteration extends ProtoField[GraphAlteration] {

  sealed trait GraphUpdate extends GraphAlteration {
    val updateTime: Long
    val index: Long
    val srcId: Long
  }

  object GraphUpdate extends ProtoField[GraphUpdate]

  /** basic update types */
  case class VertexAdd(
      updateTime: Long,
      index: Long,            // the secondary index, automatically incremented for every update to avoid duplicates
      srcId: Long,            // the actual id of the vertex
      properties: Properties,
      vType: Option[Type]
  )(implicit val provider: SchemaProvider[VertexAdd])
          extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)

  case class EdgeAdd(
      updateTime: Long,
      index: Long, // the secondary index, automatically incremented for every update to avoid duplicates
      srcId: Long, // the actual id of the source vertex
      dstId: Long, // the actual id of the destination vertex
      properties: Properties,
      eType: Option[Type]
  )(implicit val provider: SchemaProvider[EdgeAdd])
          extends GraphUpdate
}

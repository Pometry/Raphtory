package com.raphtory.internals.graph

import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.components.querymanager._

sealed private[raphtory] trait GraphAlteration {
  val sourceID: Int
}

private[raphtory] object GraphAlteration {

  sealed trait GraphUpdate extends GraphAlteration {
    val updateTime: Long
    val index: Long
    val srcId: Long
  }

  /** basic update types */
  case class VertexAdd(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vType: Option[Type]
  )(implicit val provider: SchemaProvider[VertexAdd])
          extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)

  case class VertexDelete(sourceID: Int, updateTime: Long, index: Long, srcId: Long)(implicit
      val provider: SchemaProvider[VertexDelete]
  ) extends GraphUpdate

  case class EdgeAdd(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      eType: Option[Type]
  )(implicit val provider: SchemaProvider[EdgeAdd])
          extends GraphUpdate

  case class EdgeDelete(sourceID: Int, updateTime: Long, index: Long, srcId: Long, dstId: Long)(implicit
      val provider: SchemaProvider[EdgeDelete]
  ) extends GraphUpdate

  /** Required sync after an update has been applied to a partition */
  sealed abstract class GraphUpdateEffect(val updateId: Long) extends GraphAlteration {
    val updateTime: Long
    val index: Long
  }

  case class SyncNewEdgeAdd(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      removals: List[(Long, Long)],
      vType: Option[Type]
  )(implicit
      val provider: SchemaProvider[SyncNewEdgeAdd]
  ) extends GraphUpdateEffect(dstId)

  case class BatchAddRemoteEdge(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      vType: Option[Type]
  )(implicit
      val provider: SchemaProvider[BatchAddRemoteEdge]
  ) extends GraphUpdateEffect(dstId)
          with GraphUpdate

  case class SyncExistingEdgeAdd(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  )(implicit
      val provider: SchemaProvider[SyncExistingEdgeAdd]
  ) extends GraphUpdateEffect(dstId)

  case class SyncExistingEdgeRemoval(sourceID: Int,updateTime: Long, index: Long, srcId: Long, dstId: Long)(implicit
      val provider: SchemaProvider[SyncExistingEdgeRemoval]
  ) extends GraphUpdateEffect(dstId)

  case class SyncNewEdgeRemoval(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      removals: List[(Long, Long)]
  )(implicit
      val provider: SchemaProvider[SyncNewEdgeRemoval]
  ) extends GraphUpdateEffect(dstId)

  /** Edge removals generated via vertex removals */
  case class OutboundEdgeRemovalViaVertex(sourceID: Int, updateTime: Long, index: Long, srcId: Long, dstId: Long)(implicit
      val provider: SchemaProvider[OutboundEdgeRemovalViaVertex]
  ) extends GraphUpdateEffect(dstId)

  case class InboundEdgeRemovalViaVertex(sourceID: Int, updateTime: Long, index: Long, srcId: Long, dstId: Long)(implicit
      val provider: SchemaProvider[InboundEdgeRemovalViaVertex]
  ) extends GraphUpdateEffect(srcId)

  /** Responses from a partition receiving any of the above */
  case class SyncExistingRemovals(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      removals: List[(Long, Long)],
      fromAddition: Boolean
  )(implicit
      val provider: SchemaProvider[SyncExistingRemovals]
  ) extends GraphUpdateEffect(srcId)

  case class EdgeSyncAck(
      sourceID: Int,
      updateTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      fromAddition: Boolean
  )(implicit
      val provider: SchemaProvider[EdgeSyncAck]
  ) extends GraphUpdateEffect(srcId)

  case class VertexRemoveSyncAck(sourceID: Int, updateTime: Long, index: Long, override val updateId: Long)(implicit
      val provider: SchemaProvider[VertexRemoveSyncAck]
  ) extends GraphUpdateEffect(updateId)
}

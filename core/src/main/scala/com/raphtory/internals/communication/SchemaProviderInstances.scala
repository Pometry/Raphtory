package com.raphtory.internals.communication

import com.raphtory.internals.communication.repositories.ArrowFlightRepository.ArrowSchemaProviderInstances._
import com.raphtory.internals.components.querymanager.SchemaProvider
import com.raphtory.internals.graph.GraphAlteration._

object SchemaProviderInstances {
  implicit def genericSchemaProvider[T]: SchemaProvider[T] = new SchemaProvider[T] {}

  implicit lazy val intSchemaProvider: SchemaProvider[Int] =
    intArrowFlightMessageSchemaProvider

  implicit lazy val floatSchemaProvider: SchemaProvider[Float] =
    floatArrowFlightMessageSchemaProvider

  implicit lazy val doubleSchemaProvider: SchemaProvider[Double] =
    doubleArrowFlightMessageSchemaProvider

  implicit lazy val longSchemaProvider: SchemaProvider[Long] =
    longArrowFlightMessageSchemaProvider

  implicit lazy val charSchemaProvider: SchemaProvider[Char] =
    charArrowFlightMessageSchemaProvider

  implicit lazy val stringSchemaProvider: SchemaProvider[String] =
    stringArrowFlightMessageSchemaProvider

  implicit lazy val booleanSchemaProvider: SchemaProvider[Boolean] =
    booleanArrowFlightMessageSchemaProvider

  implicit lazy val vertexAddSchemaProvider: SchemaProvider[VertexAdd] =
    vertexAddArrowFlightMessageSchemaProvider

  implicit lazy val edgeAddSchemaProvider: SchemaProvider[EdgeAdd] =
    edgeAddArrowFlightMessageSchemaProvider

  implicit lazy val vertexDeleteSchemaProvider: SchemaProvider[VertexDelete] =
    vertexDeleteArrowFlightMessageSchemaProvider

  implicit lazy val edgeDeleteSchemaProvider: SchemaProvider[EdgeDelete] =
    edgeDeleteArrowFlightMessageSchemaProvider

  implicit lazy val syncNewEdgeAddSchemaProvider: SchemaProvider[SyncNewEdgeAdd] =
    syncNewEdgeAddArrowFlightMessageSchemaProvider

  implicit lazy val batchAddRemoteEdgeSchemaProvider: SchemaProvider[BatchAddRemoteEdge] =
    batchAddRemoteEdgeArrowFlightMessageSchemaProvider

  implicit lazy val syncExistingEdgeAddSchemaProvider: SchemaProvider[SyncExistingEdgeAdd] =
    syncExistingEdgeAddArrowFlightMessageSchemaProvider

  implicit lazy val syncExistingEdgeRemovalSchemaProvider: SchemaProvider[SyncExistingEdgeRemoval] =
    syncExistingEdgeRemovalArrowFlightMessageSchemaProvider

  implicit lazy val syncNewEdgeRemovalSchemaProvider: SchemaProvider[SyncNewEdgeRemoval] =
    syncNewEdgeRemovalArrowFlightMessageSchemaProvider

  implicit lazy val outboundEdgeRemovalViaVertexSchemaProvider: SchemaProvider[OutboundEdgeRemovalViaVertex] =
    outboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider

  implicit lazy val inboundEdgeRemovalViaVertexSchemaProvider: SchemaProvider[InboundEdgeRemovalViaVertex] =
    inboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider

  implicit lazy val syncExistingRemovalsSchemaProvider: SchemaProvider[SyncExistingRemovals] =
    syncExistingRemovalsArrowFlightMessageSchemaProvider

  implicit lazy val edgeSyncAckSchemaProvider: SchemaProvider[EdgeSyncAck] =
    edgeSyncAckArrowFlightMessageSchemaProvider

  implicit lazy val vertexRemoveSyncAckSchemaProvider: SchemaProvider[VertexRemoveSyncAck] =
    vertexRemoveSyncAckArrowFlightMessageSchemaProvider
}

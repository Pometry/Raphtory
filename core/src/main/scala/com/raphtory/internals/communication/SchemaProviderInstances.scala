package com.raphtory.internals.communication

import com.raphtory.internals.FeatureToggles
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.ArrowSchemaProviderInstances._
import com.raphtory.internals.components.querymanager.SchemaProvider
import com.raphtory.internals.graph.GraphAlteration._

object SchemaProviderInstances {
  implicit def genericSchemaProvider[T]: SchemaProvider[T] = new SchemaProvider[T] {}

  implicit lazy val intSchemaProvider: SchemaProvider[Int] = {
    if (FeatureToggles.isFlightEnabled)
      intArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val floatSchemaProvider: SchemaProvider[Float] = {
    if (FeatureToggles.isFlightEnabled)
      floatArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val doubleSchemaProvider: SchemaProvider[Double] = {
    if (FeatureToggles.isFlightEnabled)
      doubleArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val longSchemaProvider: SchemaProvider[Long] = {
    if (FeatureToggles.isFlightEnabled)
      longArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val charSchemaProvider: SchemaProvider[Char] = {
    if (FeatureToggles.isFlightEnabled)
      charArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val stringSchemaProvider: SchemaProvider[String] = {
    if (FeatureToggles.isFlightEnabled)
      stringArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val booleanSchemaProvider: SchemaProvider[Boolean] = {
    if (FeatureToggles.isFlightEnabled)
      booleanArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val vertexAddSchemaProvider: SchemaProvider[VertexAdd] = {
    if (FeatureToggles.isFlightEnabled)
      vertexAddArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val edgeAddSchemaProvider: SchemaProvider[EdgeAdd] = {
    if (FeatureToggles.isFlightEnabled)
      edgeAddArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val vertexDeleteSchemaProvider: SchemaProvider[VertexDelete] = {
    if (FeatureToggles.isFlightEnabled)
      vertexDeleteArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val edgeDeleteSchemaProvider: SchemaProvider[EdgeDelete] = {
    if (FeatureToggles.isFlightEnabled)
      edgeDeleteArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val syncNewEdgeAddSchemaProvider: SchemaProvider[SyncNewEdgeAdd] = {
    if (FeatureToggles.isFlightEnabled)
      syncNewEdgeAddArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val batchAddRemoteEdgeSchemaProvider: SchemaProvider[BatchAddRemoteEdge] = {
    if (FeatureToggles.isFlightEnabled)
      batchAddRemoteEdgeArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val syncExistingEdgeAddSchemaProvider: SchemaProvider[SyncExistingEdgeAdd] = {
    if (FeatureToggles.isFlightEnabled)
      syncExistingEdgeAddArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val syncExistingEdgeRemovalSchemaProvider
      : SchemaProvider[SyncExistingEdgeRemoval] = {
    if (FeatureToggles.isFlightEnabled)
      syncExistingEdgeRemovalArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val syncNewEdgeRemovalSchemaProvider: SchemaProvider[SyncNewEdgeRemoval] = {
    if (FeatureToggles.isFlightEnabled)
      syncNewEdgeRemovalArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val outboundEdgeRemovalViaVertexSchemaProvider
      : SchemaProvider[OutboundEdgeRemovalViaVertex] = {
    if (FeatureToggles.isFlightEnabled)
      outboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val inboundEdgeRemovalViaVertexSchemaProvider
      : SchemaProvider[InboundEdgeRemovalViaVertex] = {
    if (FeatureToggles.isFlightEnabled)
      inboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val syncExistingRemovalsSchemaProvider: SchemaProvider[SyncExistingRemovals] = {
    if (FeatureToggles.isFlightEnabled)
      syncExistingRemovalsArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val edgeSyncAckSchemaProvider: SchemaProvider[EdgeSyncAck] = {
    if (FeatureToggles.isFlightEnabled)
      edgeSyncAckArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }

  implicit lazy val vertexRemoveSyncAckSchemaProvider: SchemaProvider[VertexRemoveSyncAck] = {
    if (FeatureToggles.isFlightEnabled)
      vertexRemoveSyncAckArrowFlightMessageSchemaProvider
    else genericSchemaProvider
  }
}

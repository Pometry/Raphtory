package com.raphtory.internals.communication.repositories

import com.raphtory.arrowmessaging.ArrowFlightMessageSignatureRegistry
import com.raphtory.internals.communication.models._
import com.raphtory.internals.communication.models.graphalterations._
import com.raphtory.internals.communication.models.vertexmessaging._
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphAlteration._

/** @DoNotDocument */
object ArrowFlightRepository {

  private[raphtory] val signatureRegistry = ArrowFlightMessageSignatureRegistry()

  object ArrowSchemaProviderInstances {

    private[raphtory] lazy val vertexMessagesSyncArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[VertexMessagesSync] {
        override val endpoint = "vertexMessagesSync"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexMessagesSyncArrowFlightMessage]
        )
      }

    private[raphtory] lazy val filteredEdgeMessageArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[FilteredEdgeMessage[_]] {
        override val endpoint = "filteredEdgeMessage"

        signatureRegistry.registerSignature(
          endpoint,
          classOf[FilteredEdgeMessageArrowFlightMessage]
        )
      }

    private[raphtory] lazy val filteredInEdgeMessageArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[FilteredInEdgeMessage[_]] {
        override val endpoint = "filteredInEdgeMessage"

        signatureRegistry.registerSignature(
          endpoint,
          classOf[FilteredInEdgeMessageArrowFlightMessage]
        )
      }

    private[raphtory] lazy val filteredOutEdgeMessageArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[FilteredOutEdgeMessage[_]] {
        override val endpoint = "filteredOutEdgeMessage"

        signatureRegistry.registerSignature(
          endpoint,
          classOf[FilteredOutEdgeMessageArrowFlightMessage]
        )
      }

    private[raphtory] lazy val intArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Int] {
        override val endpoint = "int"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[IntArrowFlightMessage]
        )
      }

    private[raphtory] lazy val floatArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Float] {
        override val endpoint = "float"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[FloatArrowFlightMessage]
        )
      }

    private[raphtory] lazy val doubleArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Double] {
        override val endpoint = "double"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[DoubleArrowFlightMessage]
        )
      }

    private[raphtory] lazy val longArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Long] {
        override val endpoint = "long"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[LongArrowFlightMessage]
        )
      }

    private[raphtory] lazy val charArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Char] {
        override val endpoint = "char"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[CharArrowFlightMessage]
        )
      }

    private[raphtory] lazy val stringArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[String] {
        override val endpoint = "string"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[StringArrowFlightMessage]
        )
      }

    private[raphtory] lazy val booleanArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[Boolean] {
        override val endpoint = "boolean"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[BooleanArrowFlightMessage]
        )
      }

    private[raphtory] lazy val vertexAddArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[VertexAdd] {
        override val endpoint = "vertexAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexAddArrowFlightMessage]
        )
      }

    private[raphtory] lazy val edgeAddArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[EdgeAdd] {
        override val endpoint = "edgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeAddArrowFlightMessage]
        )
      }

    private[raphtory] lazy val vertexDeleteArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[VertexDelete] {
        override val endpoint = "vertexDelete"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexDeleteArrowFlightMessage]
        )
      }

    private[raphtory] lazy val edgeDeleteArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[EdgeDelete] {
        override val endpoint = "edgeDelete"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeDeleteArrowFlightMessage]
        )
      }

    private[raphtory] lazy val syncNewEdgeAddArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[SyncNewEdgeAdd] {
        override val endpoint = "syncNewEdgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncNewEdgeAddArrowFlightMessage]
        )
      }

    private[raphtory] lazy val batchAddRemoteEdgeArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[BatchAddRemoteEdge] {
        override val endpoint = "batchAddRemoteEdge"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[BatchAddRemoteEdgeArrowFlightMessage]
        )
      }

    private[raphtory] lazy val syncExistingEdgeAddArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[SyncExistingEdgeAdd] {
        override val endpoint = "syncExistingEdgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingEdgeAddArrowFlightMessage]
        )
      }

    private[raphtory] lazy val syncExistingEdgeRemovalArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[SyncExistingEdgeRemoval] {
        override val endpoint = "syncExistingEdgeRemoval"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingEdgeRemovalArrowFlightMessage]
        )
      }

    private[raphtory] lazy val syncNewEdgeRemovalArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[SyncNewEdgeRemoval] {
        override val endpoint = "syncNewEdgeRemoval"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncNewEdgeRemovalArrowFlightMessage]
        )
      }

    private[raphtory] lazy val outboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[OutboundEdgeRemovalViaVertex] {
        override val endpoint = "outboundEdgeRemovalViaVertex"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[OutboundEdgeRemovalViaVertexArrowFlightMessage]
        )
      }

    private[raphtory] lazy val inboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[InboundEdgeRemovalViaVertex] {
        override val endpoint = "inboundEdgeRemovalViaVertex"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[InboundEdgeRemovalViaVertexArrowFlightMessage]
        )
      }

    private[raphtory] lazy val syncExistingRemovalsArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[SyncExistingRemovals] {
        override val endpoint = "syncExistingRemovals"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingRemovalsArrowFlightMessage]
        )
      }

    private[raphtory] lazy val edgeSyncAckArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[EdgeSyncAck] {
        override val endpoint = "edgeSyncAck"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeSyncAckArrowFlightMessage]
        )
      }

    private[raphtory] lazy val vertexRemoveSyncAckArrowFlightMessageSchemaProvider =
      new ArrowFlightSchemaProvider[VertexRemoveSyncAck] {
        override val endpoint = "vertexRemoveSyncAck"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexRemoveSyncAckArrowFlightMessage]
        )
      }
  }

}

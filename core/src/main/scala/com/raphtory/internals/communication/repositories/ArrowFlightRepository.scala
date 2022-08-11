package com.raphtory.internals.communication.repositories

import akka.actor.typed._
import cats.effect.Async
import cats.effect.Resource
import com.raphtory.arrowmessaging.ArrowFlightMessageSignatureRegistry
import com.raphtory.internals.communication.connectors._
import com.raphtory.internals.communication._
import com.raphtory.internals.communication.models._
import com.raphtory.internals.communication.models.graphalterations._
import com.raphtory.internals.components.querymanager.ArrowFlightSchemaProvider
import com.raphtory.internals.graph.GraphAlteration._
import com.typesafe.config.Config

/** @DoNotDocument */
object ArrowFlightRepository {
  private lazy val actorSystem = ActorSystem(SpawnProtocol(), "spawner")

  val signatureRegistry = ArrowFlightMessageSignatureRegistry()

  object ArrowSchemaProviderInstances {

    lazy val intArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Int] =
      new ArrowFlightSchemaProvider[Int] {
        override val endpoint = "int"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[IntArrowFlightMessage]
        )
      }

    lazy val floatArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Float] =
      new ArrowFlightSchemaProvider[Float] {
        override val endpoint = "float"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[FloatArrowFlightMessage]
        )
      }

    lazy val doubleArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Double] =
      new ArrowFlightSchemaProvider[Double] {
        override val endpoint = "double"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[DoubleArrowFlightMessage]
        )
      }

    lazy val longArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Long] =
      new ArrowFlightSchemaProvider[Long] {
        override val endpoint = "long"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[LongArrowFlightMessage]
        )
      }

    lazy val charArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Char] =
      new ArrowFlightSchemaProvider[Char] {
        override val endpoint = "char"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[CharArrowFlightMessage]
        )
      }

    lazy val stringArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[String] =
      new ArrowFlightSchemaProvider[String] {
        override val endpoint = "string"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[StringArrowFlightMessage]
        )
      }

    lazy val booleanArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[Boolean] =
      new ArrowFlightSchemaProvider[Boolean] {
        override val endpoint = "boolean"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[BooleanArrowFlightMessage]
        )
      }

    lazy val vertexAddArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[VertexAdd] =
      new ArrowFlightSchemaProvider[VertexAdd] {
        override val endpoint = "vertexAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexAddArrowFlightMessage]
        )
      }

    lazy val edgeAddArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[EdgeAdd] =
      new ArrowFlightSchemaProvider[EdgeAdd] {
        override val endpoint = "edgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeAddArrowFlightMessage]
        )
      }

    lazy val vertexDeleteArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[VertexDelete] =
      new ArrowFlightSchemaProvider[VertexDelete] {
        override val endpoint = "vertexDelete"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexDeleteArrowFlightMessage]
        )
      }

    lazy val edgeDeleteArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[EdgeDelete] =
      new ArrowFlightSchemaProvider[EdgeDelete] {
        override val endpoint = "edgeDelete"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeDeleteArrowFlightMessage]
        )
      }

    lazy val syncNewEdgeAddArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[SyncNewEdgeAdd] =
      new ArrowFlightSchemaProvider[SyncNewEdgeAdd] {
        override val endpoint = "syncNewEdgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncNewEdgeAddArrowFlightMessage]
        )
      }

    lazy val batchAddRemoteEdgeArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[BatchAddRemoteEdge] =
      new ArrowFlightSchemaProvider[BatchAddRemoteEdge] {
        override val endpoint = "batchAddRemoteEdge"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[BatchAddRemoteEdgeArrowFlightMessage]
        )
      }

    lazy val syncExistingEdgeAddArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[SyncExistingEdgeAdd] =
      new ArrowFlightSchemaProvider[SyncExistingEdgeAdd] {
        override val endpoint = "syncExistingEdgeAdd"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingEdgeAddArrowFlightMessage]
        )
      }

    lazy val syncExistingEdgeRemovalArrowFlightMessageSchemaProvider
        : ArrowFlightSchemaProvider[SyncExistingEdgeRemoval] =
      new ArrowFlightSchemaProvider[SyncExistingEdgeRemoval] {
        override val endpoint = "syncExistingEdgeRemoval"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingEdgeRemovalArrowFlightMessage]
        )
      }

    lazy val syncNewEdgeRemovalArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[SyncNewEdgeRemoval] =
      new ArrowFlightSchemaProvider[SyncNewEdgeRemoval] {
        override val endpoint = "syncNewEdgeRemoval"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncNewEdgeRemovalArrowFlightMessage]
        )
      }

    lazy val outboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider
        : ArrowFlightSchemaProvider[OutboundEdgeRemovalViaVertex] =
      new ArrowFlightSchemaProvider[OutboundEdgeRemovalViaVertex] {
        override val endpoint = "outboundEdgeRemovalViaVertex"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[OutboundEdgeRemovalViaVertexArrowFlightMessage]
        )
      }

    lazy val inboundEdgeRemovalViaVertexArrowFlightMessageSchemaProvider
        : ArrowFlightSchemaProvider[InboundEdgeRemovalViaVertex] =
      new ArrowFlightSchemaProvider[InboundEdgeRemovalViaVertex] {
        override val endpoint = "inboundEdgeRemovalViaVertex"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[InboundEdgeRemovalViaVertexArrowFlightMessage]
        )
      }

    lazy val syncExistingRemovalsArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[SyncExistingRemovals] =
      new ArrowFlightSchemaProvider[SyncExistingRemovals] {
        override val endpoint = "syncExistingRemovals"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[SyncExistingRemovalsArrowFlightMessage]
        )
      }

    lazy val edgeSyncAckArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[EdgeSyncAck] =
      new ArrowFlightSchemaProvider[EdgeSyncAck] {
        override val endpoint = "edgeSyncAck"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[EdgeSyncAckArrowFlightMessage]
        )
      }

    lazy val vertexRemoveSyncAckArrowFlightMessageSchemaProvider: ArrowFlightSchemaProvider[VertexRemoveSyncAck] =
      new ArrowFlightSchemaProvider[VertexRemoveSyncAck] {
        override val endpoint = "vertexRemoveSyncAck"

        signatureRegistry.registerSignature(
                endpoint,
                classOf[VertexRemoveSyncAckArrowFlightMessage]
        )
      }
  }

  def apply[IO[_]: Async](config: Config): Resource[IO, TopicRepository] =
    for {
      pulsarConnector <- PulsarConnector[IO](config)
      akkaConnector   <- AkkaConnector[IO](AkkaConnector.StandaloneMode, config)
    } yield {
      val arrowFlightConnector = new ArrowFlightConnector(config, signatureRegistry)
      new TopicRepository(pulsarConnector, pulsarConnector, config) {
        override def jobOperationsConnector: Connector    = akkaConnector
        override def jobStatusConnector: Connector        = akkaConnector
        override def queryPrepConnector: Connector        = akkaConnector
        override def completedQueriesConnector: Connector = akkaConnector
        override def watermarkConnector: Connector        = akkaConnector
        override def rechecksConnector: Connector         = akkaConnector
        override def queryTrackConnector: Connector       = akkaConnector
        override def submissionsConnector: Connector      = akkaConnector
        override def graphSyncConnector: Connector        = arrowFlightConnector
        override def graphUpdatesConnector: Connector     = arrowFlightConnector
        override def vertexMessagesConnector: Connector   = arrowFlightConnector
      }
    }
}

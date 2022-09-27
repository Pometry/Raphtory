package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.context.LocalContext.createName
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.management.id.LocalIDManager
import com.typesafe.config.Config
import fs2.Stream

trait QueryUpdate

trait RService[F[_]] {
  def establishGraph(clientID: String, graphID: String): F[Unit]
  def submitQuery(query: Query): Stream[F, QueryUpdate]
  def submitSource(ingestData: IngestData): F[Unit]
  def disconnect(clientID: String, graphID: String): F[Unit]
  def destroyGraph(clientID: String, graphID: String, force: Boolean): F[Unit]
  def getNextAvailableId(pool: String): F[Int]
  def sendUpdate(graphID: String, update: GraphUpdate): F[Unit]

  def unblockIngestion(
      graphID: String,
      sourceID: Int,
      messageCount: Long,
      highestTimeSeen: Long,
      force: Boolean
  ): F[Unit]
}

class DefaultRService[F[_]](idManager: IDManager, repo: TopicRepository, config: Config)(implicit F: Async[F])
        extends RService[F] {
  private val partitioner            = new Partitioner(config)
  private lazy val blockingIngestion = repo.blockingIngestion().endPoint
  private lazy val graphSetup        = repo.graphSetup.endPoint
  private lazy val submissions       = repo.submissions().endPoint
  private lazy val ingestion         = repo.ingestSetup.endPoint

  private lazy val writers =
    Map[String, Map[Int, EndPoint[GraphAlteration]]]().withDefault(graphID =>
      repo.graphUpdates(graphID).endPoint
    ) // This is to cache endpoints instead of creating one for every update we send

  override def establishGraph(clientID: String,): F[String] =
    F.delay(graphSetup sendAsync EstablishGraph(graphID, clientID)) // TODO: shouldn't this return the graph ID?

  override def submitQuery(query: Query): F[Stream[F, QueryUpdate]] = submissions sendAsync query

  override def submitSource(ingestData: IngestData): F[Unit] = F.delay(ingestion sendAsync ingestData)

  override def disconnect(clientID: String, graphID: String): F[Unit] =
    F.delay(graphSetup sendAsync ClientDisconnected(graphID, clientID))

  override def destroyGraph(clientID: String, graphID: String, force: Boolean): F[Unit] =
    F.delay(graphSetup sendAsync DestroyGraph(graphID, clientID, force))

  override def getNextAvailableId(pool: String): F[Int] = F.blocking(idManager.getNextAvailableID(pool))

  override def sendUpdate(graphID: String, update: GraphUpdate): F[Unit] =
    F.delay(writers(graphID)(partitioner.getPartitionForId(update.srcId)) sendAsync update)

  override def unblockIngestion(
      graphID: String,
      sourceID: Int,
      messageCount: Long,
      highestTimeSeen: Long,
      force: Boolean
  ): F[Unit] =
    for {
      message <- F.pure(UnblockIngestion(sourceID, graphID = graphID, messageCount, highestTimeSeen, force))
      _       <- F.delay(blockingIngestion sendAsync message)
    } yield ()
}

object RService {

  def apply[F[_]](config: Config)(implicit F: Async[F]): Resource[F, RService[F]] =
    for {
      prometheusPort     <- Resource.pure(config.getInt("raphtory.prometheus.metrics.port"))
      scheduler          <- Resource.pure(new Scheduler())
      partitionIdManager <- Resource.pure(new LocalIDManager())
      sourceIdManager    <- Resource.pure(new LocalIDManager())
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      repo               <- LocalTopicRepository(config)
      _                  <- PartitionOrchestrator.spawn[IO](config, partitionIdManager, graphID, repo, scheduler)
      _                  <- IngestionManager[IO](config, repo)
      _                  <- QueryManager[IO](config, repo)
      service            <- Resource.eval(F.delay(new DefaultRService[F](sourceIdManager, repo, config)))
    } yield service
}

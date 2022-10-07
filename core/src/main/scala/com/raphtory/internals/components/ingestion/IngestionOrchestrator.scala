package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.std.Queue
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.TryIngestData
import com.raphtory.internals.management.Scheduler
import com.raphtory.protocol.ClientGraphId
import com.raphtory.protocol.DestroyGraph
import com.raphtory.protocol.Status
import com.typesafe.config.Config
import fs2.Stream

import scala.util.Success

//class IngestionOrchestrator(repo: TopicRepository, conf: Config) extends OrchestratorComponent(conf) {
//
//  override private[raphtory] def run(): Unit =
//    logger.info(s"Starting Ingestion Service")
//
//  override def handleMessage(msg: ClusterManagement): Unit =
//    msg match {
//      case EstablishGraph(graphID: String, clientID: String) =>
//        establishService("Ingestion Manager", graphID, clientID, repo, deployIngestionService)
//      case DestroyGraph(graphID, clientID, force)            => destroyGraph(graphID, clientID, force)
//      case ClientDisconnected(graphID, clientID)             => clientDisconnected(graphID, clientID)
//    }
//}
//
//object IngestionOrchestrator {
//
//  def apply[IO[_]: Async: Spawn](
//      conf: Config,
//      topics: TopicRepository
//  ): Resource[IO, IngestionOrchestrator] =
//    Component.makeAndStart(
//            topics,
//            s"ingestion-node",
//            List(topics.clusterComms),
//            new IngestionOrchestrator(topics, conf)
//    )
//}

trait IngestionOrchestrator[F[_]] {
  def establishGraph(info: ClientGraphId): F[Status]
  def destroyGraph(request: DestroyGraph): F[Status]
  def disconnectClient(info: ClientGraphId): F[Status]
  def ingestData(ingestData: TryIngestData): F[Status]
}

class DefaultIngestionOrchestrator[F[_]: Async](
    queues: Ref[F, Map[String, Queue[F, Option[IngestData]]]],
    supervisor: Supervisor[F],
    repo: TopicRepository,
    config: Config
) extends IngestionOrchestrator[F] {

  override def establishGraph(info: ClientGraphId): F[Status] =
    info match {
      case ClientGraphId(clientId, graphId, _) =>
        for {
          queue <- Queue.unbounded[F, Option[IngestData]]
          _     <- supervisor.supervise(processQueue(queue))
          _     <- queues.update(_ + (graphId -> queue)) // TODO: Check if we already have it, use modify
        } yield (Status(success = true)) // TODO: here we can report something else
    }

  override def destroyGraph(request: DestroyGraph): F[Status] =
    request match {
      case DestroyGraph(clientId, graphId, force, _) =>
        for {
          queue <- queues.get.map(_(graphId)) // this can throw an exception
          _     <- queue offer None
          _     <- queues.update(map => map.removed(graphId))
        } yield (Status(success = true)) // TODO: here we can report something else
    }

  override def disconnectClient(info: ClientGraphId): F[Status] =
    info match {
      case ClientGraphId(clientId, graphId, _) => destroyGraph(DestroyGraph(graphId, clientId, force = false))
    }

  override def ingestData(request: TryIngestData): F[Status] =
    request match {
      case TryIngestData(Success(ingestData)) =>
        for {
          queue <- queues.get.map(_(ingestData.graphID)) // this can throw an exception
          _     <- queue offer Some(ingestData)
        } yield (Status(success = true)) // TODO: here we can report something else
    }

  private def processQueue(queue: Queue[F, Option[IngestData]]): F[Unit] =
    (for {
      graphSupervisor <- Stream.resource(Supervisor[F])
      request         <- Stream.fromQueueNoneTerminated(queue, 1000)
      _               <- Stream.eval(graphSupervisor.supervise(processSource(request)))
    } yield ()).compile.drain

  private def processSource(req: IngestData): F[Unit] =
    req match {
      case IngestData(_, graphID, sourceId, source, blocking) =>
        for {
          executor <-
            Async[F].delay(new IngestionExecutor(graphID, source, blocking, sourceId, config, repo, new Scheduler()))
          _        <- Async[F].blocking(executor.run())
        } yield ()
    }
}

object IngestionOrchestratorBuilder {

  def apply[F[_]: Async](repo: TopicRepository, config: Config): Resource[F, IngestionOrchestrator[F]] =
    for {
      queues       <- Resource.eval(Ref[F].of(Map[String, Queue[F, Option[IngestData]]]()))
      supervisor   <- Supervisor[F]
      orchestrator <-
        Resource.eval(Async[F].delay(new DefaultIngestionOrchestrator[F](queues, supervisor, repo, config)))
    } yield orchestrator
}

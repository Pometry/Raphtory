package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.OrchestratorService.Graph
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.OrchestratorService
import com.raphtory.internals.components.OrchestratorServiceBuilder
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRepository
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.TryIngestData
import com.raphtory.protocol
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.Status
import com.raphtory.protocol.failure
import com.raphtory.protocol.success
import com.typesafe.config.Config

class IngestionServiceInstance[F[_]: Async](graphs: GraphList[F, Unit], repo: TopicRepository, config: Config)
        extends OrchestratorService(graphs)
        with IngestionService[F] {

  override def makeGraphData(graphId: String): F[Unit] = Async[F].unit

  override def ingestData(request: protocol.IngestData): F[Status] =
    request match {
      case protocol.IngestData(TryIngestData(scala.util.Success(req)), _) =>
        val executor = IngestionExecutor(req.graphID, req.source, req.blocking, req.sourceId, config, repo)
        for {
          executorResource           <- executor.allocated
          (executor, releaseExecutor) = executorResource
          _                          <- attachExecutionToGraph(req.graphID, _ => runExecutor(req.graphID, executor, releaseExecutor))
        } yield success
    }

  private def runExecutor(graphId: String, executor: IngestionExecutor[F], release: F[Unit]) = {
    def logError(e: Throwable): Unit = logger.error(s"Exception while executing source: $e")
    for {
      _ <- executor.run().handleErrorWith(e => Async[F].delay(logError(e)) *> destroyGraph(graphId))
      _ <- release
    } yield ()
  }
}

object IngestionServiceInstance extends OrchestratorServiceBuilder {

  def apply[F[_]: Async](repo: ServiceRepository[F], config: Config): Resource[F, Unit] =
    for {
      graphs  <- makeGraphList[F, Unit]
      _       <- Resource.eval(Async[F].delay(logger.info(s"Starting Ingestion Service")))
      service <- Resource.eval(Async[F].delay(new IngestionServiceInstance[F](graphs, repo.topics, config)))
      _       <- repo.registered(service, IngestionServiceInstance.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, IngestionService[F]] =
    GrpcServiceDescriptor[F, IngestionService[F]](
            "ingestion",
            IngestionService.client(_),
            IngestionService.bindService(Async[F], _)
    )
}

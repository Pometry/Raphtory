package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
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

object IngestionServiceInstance extends OrchestratorServiceBuilder {

  def apply[F[_]: Async](repo: ServiceRepository[F], config: Config): Resource[F, Unit] =
    for {
      graphs  <- makeGraphList
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

class IngestionServiceInstance[F[_]: Async](graphs: GraphList[F], repo: TopicRepository, config: Config)
        extends OrchestratorService[F](graphs)
        with IngestionService[F] {

  override def ingestData(request: protocol.IngestData): F[Status] =
    request match {
      case protocol.IngestData(TryIngestData(scala.util.Success(req)), _) =>
        attachExecutionToGraph(req.graphID, processSource(req).onError(_ => destroyGraph(req.graphID))).as(success)
      case _                                                              => failure[F]
    }

  private def processSource(req: IngestData): F[Unit] =
    req match {
      case IngestData(_, graphID, sourceId, source, blocking) =>
        IngestionExecutor(graphID, source, blocking, sourceId, config, repo)
    }
}

package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components._
import com.raphtory.internals.components.querymanager._
import com.raphtory.protocol
import com.raphtory.protocol.{IngestionService, QueryService, Status, success}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

class IngestionServiceImpl[F[_]: Async] private (
    graphs: GraphList[F, Unit],
    queryService: Resource[F, QueryService[F]],
    repo: TopicRepository,
    config: Config
) extends NoGraphDataOrchestratorService(graphs)
        with IngestionService[F] {

  override def ingestData(request: protocol.IngestData): F[Status] =
    request match {
      case protocol.IngestData(TryIngestData(scala.util.Success(req)), _) =>
        val executor = IngestionExecutor(req.graphID, queryService, req.source, req.blocking, req.sourceId, config, repo)
        for {
          executorResource           <- executor.allocated
          (executor, releaseExecutor) = executorResource
          _                          <- attachExecutionToGraph(req.graphID, _ => runExecutor(req.graphID, executor, releaseExecutor))
        } yield success
    }

  private def runExecutor(graphId: String, executor: IngestionExecutor[F], release: F[Unit]): F[Unit] = {
    def logError(e: Throwable): Unit = logger.error(s"Exception while executing source: $e")
    for {
      _ <- executor.run().handleErrorWith(e => Async[F].delay(logError(e)) *> destroyGraph(graphId))
      _ <- release
    } yield ()
  }
}

object IngestionServiceImpl {
  import OrchestratorService._

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](repo: ServiceRegistry[F], config: Config): Resource[F, Unit] =
    for {
      graphs  <- makeGraphList[F, Unit]
      _       <- Resource.eval(Async[F].delay(logger.info(s"Starting Ingestion Service")))
      service <- Resource.eval(Async[F].delay(new IngestionServiceImpl[F](graphs, repo.query, repo.topics, config)))
      _       <- repo.registered(service, IngestionServiceImpl.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, IngestionService[F]] =
    GrpcServiceDescriptor[F, IngestionService[F]](
            "ingestion",
            IngestionService.client(_),
            IngestionService.bindService(Async[F], _)
    )
}

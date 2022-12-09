package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components._
import com.raphtory.internals.components.querymanager._
import com.raphtory.protocol
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.QueryService
import com.raphtory.protocol.Status
import com.raphtory.protocol.success
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

class IngestionServiceImpl[F[_]: Async] private (
    graphs: GraphList[F, Unit],
    queryService: QueryService[F],
    partitions: Map[Int, PartitionService[F]],
    config: Config
) extends NoGraphDataOrchestratorService(graphs)
        with IngestionService[F] {

  override def ingestData(request: protocol.IngestData): F[Status] =
    request match {
      case protocol.IngestData(TryIngestData(scala.util.Success(req)), _) =>
        for {
          e <- IngestionExecutor(req.graphID, queryService, req.source, req.sourceId, config, partitions)
          _ <- attachExecutionToGraph(req.graphID, _ => runExecutor(req.graphID, e))
        } yield success
    }

  private def runExecutor(graphId: String, executor: IngestionExecutor[F, _]): F[Unit] = {
    def logError(e: Throwable): Unit = logger.error(s"Exception while executing source: $e")
    executor.run()
      .handleErrorWith(e => Async[F].delay(logError(e)))
  }
}

object IngestionServiceImpl {
  import OrchestratorService._

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](registry: ServiceRegistry[F], config: Config): Resource[F, Unit] =
    for {
      graphs       <- makeGraphList[F, Unit]
      _            <- Resource.eval(Async[F].delay(logger.info(s"Starting Ingestion Service")))
      queryService <- registry.query
      partitions   <- registry.partitions
      service      <- Resource.eval(Async[F].delay(new IngestionServiceImpl[F](graphs, queryService, partitions, config)))
      _            <- registry.registered(service, IngestionServiceImpl.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, IngestionService[F]] =
    GrpcServiceDescriptor[F, IngestionService[F]](
            "ingestion",
            IngestionService.client(_),
            IngestionService.bindService(Async[F], _)
    )
}

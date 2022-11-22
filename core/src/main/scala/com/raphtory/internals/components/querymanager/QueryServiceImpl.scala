package com.raphtory.internals.components.querymanager

import cats.syntax.all._
import cats.effect._
import com.google.protobuf.empty.Empty
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components._
import com.raphtory.protocol
import com.raphtory.protocol.{PartitionService, QueryService}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import fs2.Stream
import org.slf4j.LoggerFactory

import scala.util.Failure
import scala.util.Success

class QueryServiceImpl[F[_]: Async] private (
    graphs: GraphList[F, QuerySupervisor[F]],
    topics: TopicRepository,
    config: Config,
    partitions: Map[Int, PartitionService[F]]
) extends OrchestratorService(graphs)
        with protocol.QueryService[F] {

  override protected def makeGraphData(graphID: String): Resource[F, QuerySupervisor[F]] =
    QuerySupervisor(graphID, topics, config, partitions)

  private def getQuerySupervisor(graphID: String): F[QuerySupervisor[F]] =
    for (m <- graphs.get) yield m(graphID).data

  override def blockIngestion(req: protocol.BlockIngestion): F[Empty]    =
    for {
      querySupervisor <- getQuerySupervisor(req.graphID)
      _               <- querySupervisor.startBlockingIngestion(req.sourceID)
    } yield Empty()

  override def unblockIngestion(req: protocol.UnblockIngestion): F[Empty] =
    for {
      querySupervisor <- getQuerySupervisor(req.graphID)
      _               <- querySupervisor.endBlockingIngestion(req.sourceID, req.earliestTimeSeen, req.latestTimeSeen)
    } yield Empty()

  override def submitQuery(req: protocol.Query): F[Stream[F, protocol.QueryManagement]] =
    req match {
      case protocol.Query(TryQuery(Success(query)), _) =>
        for {
          querySupervisor <- getQuerySupervisor(query.graphID)
          responses       <- querySupervisor.submitQuery(query)
        } yield responses
      case protocol.Query(TryQuery(Failure(error)), _) =>
        Stream[F, protocol.QueryManagement](protocol.QueryManagement(JobFailed(error))).pure[F]
    }
}

object QueryServiceImpl {
  import OrchestratorService._

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](repo: ServiceRegistry[F], config: Config): Resource[F, Unit] =
    for {
      graphs  <- makeGraphList[F, QuerySupervisor[F]]
      _       <- Resource.eval(Async[F].delay(logger.info(s"Starting Query Service")))
      partitions   <- repo.partitions
      service <- Resource.eval(Async[F].delay(new QueryServiceImpl[F](graphs, repo.topics, config, partitions)))
      _       <- repo.registered(service, QueryServiceImpl.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, QueryService[F]] =
    GrpcServiceDescriptor[F, QueryService[F]](
            "query",
            QueryService.client(_),
            QueryService.bindService(Async[F], _)
    )
}

package com.raphtory.internals.components.querymanager

import cats.syntax.all._
import cats.effect._
import com.google.protobuf.empty.Empty
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.OrchestratorService.GraphList
import com.raphtory.internals.components._
import com.raphtory.protocol
import com.raphtory.protocol.QueryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.util.Failure
import scala.util.Success

class QueryServiceImpl[F[_]: Async](
    graphs: GraphList[F, QuerySupervisor],
    topics: TopicRepository,
    config: Config
) extends OrchestratorService(graphs)
        with protocol.QueryService[F] {

  override protected def makeGraphData(graphID: String): F[QuerySupervisor] =
    Async[F].delay(QuerySupervisor(graphID, topics, config))

  private def getQuerySupervisor(graphID: String): F[QuerySupervisor] =
    for (m <- graphs.get) yield m(graphID).data

  override def blockIngestion(req: protocol.BlockIngestion): F[Empty] =
    for {
      querySupervisor <- getQuerySupervisor(req.graphID)
      _                = querySupervisor.startBlockingIngestion(req.sourceID)
    } yield Empty()

  override def unblockIngestion(req: protocol.UnblockIngestion): F[Empty] =
    for {
      querySupervisor <- getQuerySupervisor(req.graphID)
      _                = querySupervisor.endBlockingIngestion(req.sourceID, req.earliestTimeSeen, req.latestTimeSeen)
    } yield Empty()

  override def submitQuery(req: protocol.Query): F[Empty] =
    req match {
      case protocol.Query(TryQuery(Success(query)), _) =>
        for {
          querySupervisor <- getQuerySupervisor(query.graphID)
          _                = querySupervisor.submitQuery(query)
        } yield Empty()
      case protocol.Query(TryQuery(Failure(error)), _) =>
        throw new Exception(s"Error interpreting request $req").initCause(error)
    }

  override def endQuery(req: protocol.JobID): F[Empty] =
    for {
      querySupervisor <- getQuerySupervisor(req.jobID)
      _                = querySupervisor.endQuery(req.jobID)
    } yield Empty()
}

object QueryServiceImpl {
  import OrchestratorService._

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](repo: ServiceRegistry[F], config: Config): Resource[F, Unit] =
    for {
      graphs  <- makeGraphList[F, QuerySupervisor]
      _       <- Resource.eval(Async[F].delay(logger.info(s"Starting Query Service")))
      service <- Resource.eval(Async[F].delay(new QueryServiceImpl[F](graphs, repo.topics, config)))
      _       <- repo.registered(service, QueryServiceImpl.descriptor)
    } yield ()

  def descriptor[F[_]: Async]: ServiceDescriptor[F, QueryService[F]] =
    GrpcServiceDescriptor[F, QueryService[F]](
            "query",
            QueryService.client(_),
            QueryService.bindService(Async[F], _)
    )
}

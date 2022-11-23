package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.protobuf.empty.Empty
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceImpl
import com.raphtory.internals.components.partition.PartitionServiceImpl
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.components.repositories.DistributedServiceRegistry
import com.raphtory.internals.components.repositories.LocalServiceRegistry
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.makeLocalIdManager
import com.raphtory.protocol
import com.raphtory.protocol.GraphInfo
import com.raphtory.protocol.IdPool
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.OptionalId
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.QueryService
import com.raphtory.protocol.RaphtoryService
import com.raphtory.protocol.Status
import com.raphtory.protocol.failure
import com.raphtory.protocol.success
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import fs2.Stream
import grpc.health.v1.health.Health
import higherkindness.mu.rpc.ChannelForAddress
import higherkindness.mu.rpc.healthcheck.HealthService
import higherkindness.mu.rpc.server.AddService
import higherkindness.mu.rpc.server.GrpcServer
import org.slf4j.LoggerFactory
import scala.util.Failure
import scala.util.Success

class RaphtoryServiceImpl[F[_]](
    runningGraphs: Ref[F, Map[String, Set[String]]],
    existingGraphs: Ref[F, Set[String]],
    ingestion: IngestionService[F],
    partitions: Map[Int, PartitionService[F]],
    queryService: QueryService[F],
    idManager: IDManager,
    config: Config
)(implicit F: Async[F])
        extends RaphtoryService[F] {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val partitioner    = new Partitioner()

  override def establishGraph(req: GraphInfo): F[Status] =
    for {
      alreadyCreating <- existingGraphs.modify(graphs =>
                           if (graphs contains req.graphId) (graphs, true) else (graphs + req.graphId, false)
                         )
      status          <- if (alreadyCreating) failure[F]
                         else
                           for {
                             _ <- ingestion.establishGraph(req)
                             _ <- controlPartitions(_.establishGraph(req))
                             _ <- queryService.establishGraph(req)
                             _ <- runningGraphs.update(graphs =>
                                    if (graphs contains req.graphId) graphs else (graphs + (req.graphId -> Set(req.clientId)))
                                  )
                           } yield success
    } yield status

  override def destroyGraph(req: protocol.DestroyGraph): F[Status] =
    for {
      // TODO check first existingGraphs and return already in progress failure
      canDestroy <- if (req.force) forcedRemoval(req.graphId) else safeRemoval(req.graphId, req.clientId)
      status     <- if (!canDestroy) failure[F]
                    else
                      for {
                        _      <- F.delay(logger.debug(s"Destroying graph ${req.graphId}"))
                        _      <- ingestion.destroyGraph(GraphInfo(req.clientId, req.graphId))
                        _      <- controlPartitions(_.destroyGraph(GraphInfo(req.clientId, req.graphId)))
                        status <- queryService.destroyGraph(GraphInfo(req.clientId, req.graphId))
                        _      <- existingGraphs.update(graphs => graphs - req.graphId)
                      } yield status
    } yield status

  override def disconnect(req: GraphInfo): F[Status] =
    runningGraphs.modify { graphs =>
      val updatedGraphs = removeClientFromGraphs(graphs, req.graphId, req.clientId)
      val status        = if (graphs contains req.graphId) success else failure
      (updatedGraphs, status)
    }

  override def submitQuery(req: protocol.Query): F[Stream[F, protocol.QueryManagement]] =
    req match {
      case protocol.Query(TryQuery(Success(query)), _) =>
        for {
          graphRunning <- runningGraphs.get.map(graphs => graphs.isDefinedAt(query.graphID))
          responses    <- if (graphRunning)
                            establishExecutors(req) *> queryService.submitQuery(req)
                          else Stream[F, protocol.QueryManagement](graphNotRunningMessage(query.graphID)).pure[F]
        } yield responses
      case protocol.Query(TryQuery(Failure(error)), _) =>
        Stream[F, protocol.QueryManagement](protocol.QueryManagement(JobFailed(error))).pure[F]
    }

  private def graphNotRunningMessage(graphId: String) =
    protocol.QueryManagement(JobFailed(new IllegalStateException(s"Graph $graphId is not running")))

  private def establishExecutors(query: protocol.Query) =
    controlPartitions(_.establishExecutor(query))

  override def submitSource(req: protocol.IngestData): F[Status] = ingestion.ingestData(req)

  override def getNextAvailableId(req: IdPool): F[OptionalId] =
    F.blocking(idManager.getNextAvailableID(req.pool)).map(protocol.OptionalId(_))

  override def processUpdate(req: protocol.GraphUpdate): F[Status] =
    for {
      _ <- F.delay(logger.trace(s"Processing graph update ${req.update}"))
      _ <- partitions(partitioner.getPartitionForId(req.update.srcId)).processUpdates(
                   protocol.GraphAlterations(req.graphId, Vector(protocol.GraphAlteration(req.update)))
           )

    } yield success

  override def connectToGraph(req: GraphInfo): F[Empty] =
    runningGraphs
      .update(graphs =>
        graphs.updatedWith(req.graphId) {
          case Some(clients) => Some(clients + req.clientId)
          case None          => throw new IllegalStateException(s"The graph '${req.graphId}' doesn't exist")
        }
      )
      .as(Empty())

  private def controlPartitions[T](f: PartitionService[F] => F[T]) =
    F.parSequenceN(partitions.size)(partitions.values.toSeq.map(f))

  /** Returns true if we can destroy the graph */
  private def forcedRemoval(graphId: String): F[Boolean] =
    runningGraphs.modify(graphs => (graphs.removed(graphId), graphs contains graphId))

  /** Returns true if we can destroy the graph */
  private def safeRemoval(graphId: String, clientId: String): F[Boolean] =
    runningGraphs.modify { graphs =>
      val graphsWithClientRemoved       = removeClientFromGraphs(graphs, graphId, clientId)
      lazy val graphsWithGraphRemoved   = graphs - graphId
      val (updatedGraphs, safeToRemove) = graphsWithClientRemoved.get(graphId) match {
        case Some(clients) =>
          if (clients.isEmpty) (graphsWithGraphRemoved, true) else (graphsWithClientRemoved, false)
        case None          => (graphs, false)
      }
      (updatedGraphs, safeToRemove)
    }

  private def removeClientFromGraphs(graphs: Map[String, Set[String]], graphId: String, clientId: String) =
    graphs map {
      case (`graphId`, clients) => graphId -> (clients - clientId)
      case tuple                => tuple
    }
}

object RaphtoryServiceBuilder {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def standalone[F[_]](config: Config)(implicit F: Async[F]): Resource[F, RaphtoryService[F]] =
    createService(localCluster(config), config)

  def arrowStandalone[V: VertexSchema, E: EdgeSchema, F[_]](
      config: Config
  )(implicit F: Async[F]): Resource[F, RaphtoryService[F]] =
    createService(localArrowCluster(config), config)

  def manager[F[_]: Async](config: Config): Resource[F, RaphtoryService[F]] =
    createService(remoteCluster(config), config)

  def client[F[_]: Async](config: Config): Resource[F, RaphtoryService[F]] =
    for {
      address <- Resource.pure(config.getString("raphtory.deploy.address"))
      port    <- port(config)
      client  <- RaphtoryService.client[F](ChannelForAddress(address, port))
    } yield client

  def server[F[_]: Async](service: RaphtoryService[F], config: Config): Resource[F, Unit] = {
    implicit val implicitService: RaphtoryService[F] = service
    for {
      port          <- port(config)
      healthService <- Resource.eval(HealthService.build[F])
      healthDef     <- Health.bindService[F](Async[F], healthService)
      serviceDef    <- RaphtoryService.bindService[F]
      server        <- Resource.eval(GrpcServer.default[F](port, List(AddService(serviceDef), AddService(healthDef))))
      _             <- GrpcServer.serverResource[F](server)
      _             <- Resource.eval(Async[F].delay(logger.info("Raphtory service started")))
    } yield ()
  }

  private def createService[F[_]](cluster: Resource[F, ServiceRegistry[F]], config: Config)(implicit F: Async[F]) =
    for {
      repo            <- cluster
      sourceIDManager <- makeLocalIdManager[F]
      ingestion       <- repo.ingestion
      partitions      <- repo.partitions
      query           <- repo.query
      runningGraphs   <- Resource.eval(Ref.of(Map[String, Set[String]]()))
      existingGraphs  <- Resource.eval(Ref.of(Set[String]()))
      service         <- Resource.eval(
                                 Async[F].delay(
                                         new RaphtoryServiceImpl(
                                                 runningGraphs,
                                                 existingGraphs,
                                                 ingestion,
                                                 partitions,
                                                 query,
                                                 sourceIDManager,
                                                 config
                                         )
                                 )
                         )
    } yield service

  private def localCluster[F[_]: Async](config: Config): Resource[F, ServiceRegistry[F]] =
    for {
      registry <- LocalServiceRegistry()
      _        <- PartitionServiceImpl.makeN(registry, config)
      _        <- QueryServiceImpl(registry, config)
      _        <- IngestionServiceImpl(registry, config)
    } yield registry

  private def localArrowCluster[F[_]: Async, V: VertexSchema, E: EdgeSchema](
      config: Config
  ): Resource[F, ServiceRegistry[F]] =
    for {
      registry <- LocalServiceRegistry()
      _        <- PartitionServiceImpl.makeNArrow[F, V, E](registry, config)
      _        <- QueryServiceImpl(registry, config)
      _        <- IngestionServiceImpl(registry, config)
    } yield registry

  private def remoteCluster[F[_]: Async](config: Config): Resource[F, ServiceRegistry[F]] =
    DistributedServiceRegistry(config)

  private def port[F[_]](config: Config): Resource[F, Int] = Resource.pure(config.getInt("raphtory.deploy.port"))
}

class QueryTrackerForwarder[F[_]](queue: Queue[F, Option[QueryManagement]], dispatcher: Dispatcher[F], config: Config)
        extends Component[QueryManagement](config) {
  private val log: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  override private[raphtory] def run(): Unit = log.debug("Running QueryTrackerForwarder")

  override def handleMessage(msg: QueryManagement): Unit = {
    log.trace(s"Putting message $msg into the queue")
    dispatcher.unsafeRunSync(queue.offer(Some(msg)))
    //    if (msg == JobDone)
    //      dispatcher.unsafeRunSync(queue.offer(None))
    // TODO: Merge output and querytracking topics and turn back on
  }
  override private[raphtory] def stop(): Unit = log.debug("QueryTrackerListener stopping")
}

object QueryTrackerForwarder {
  private val log: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]](
      graphID: String,
      jobId: String,
      repo: TopicRepository,
      queue: Queue[F, Option[QueryManagement]],
      dispatcher: Dispatcher[F],
      config: Config
  )(implicit
      F: Async[F]
  ): Resource[F, QueryTrackerForwarder[F]] =
    Component
      .makeAndStart(
              repo,
              s"query-tracker-listener-$jobId",
              Seq(repo.queryTrack(graphID, jobId), repo.output(graphID, jobId)),
              new QueryTrackerForwarder(queue, dispatcher, config)
      )
      .map { component =>
        log.debug(s"Created QueryTrackerForwarder for job: $jobId")
        component
      }
}

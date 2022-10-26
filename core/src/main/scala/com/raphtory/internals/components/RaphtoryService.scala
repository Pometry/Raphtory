package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.syntax.all._
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceInstance
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.components.repositories.DistributedServiceRepository
import com.raphtory.internals.components.repositories.LocalServiceRepository
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.makeLocalIdManager
import com.raphtory.protocol
import com.raphtory.protocol.ClientGraphId
import com.raphtory.protocol.GetGraph
import com.raphtory.protocol.IdPool
import com.raphtory.protocol.OptionalId
import com.raphtory.protocol.RaphtoryService
import com.raphtory.protocol.Status
import com.raphtory.protocol
import com.raphtory.protocol.ClientGraphId
import com.raphtory.protocol.IdPool
import com.raphtory.protocol.OptionalId
import com.raphtory.protocol.IngestionService
import com.raphtory.protocol.RaphtoryService
import com.raphtory.protocol.Status
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

class DefaultRaphtoryService[F[_]](
    ingestion: IngestionService[F],
    idManager: IDManager,
    topics: TopicRepository,
    config: Config
)(implicit
    F: Async[F]
) extends RaphtoryService[F] {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val partitioner    = new Partitioner(config)

  private lazy val cluster = topics.clusterComms.endPoint

  // This is to cache endpoints instead of creating one for every message we send
  private lazy val submissions = Map[String, EndPoint[Submission]]().withDefault(topics.submissions(_).endPoint)

  private lazy val blockingIngestion =
    Map[String, EndPoint[IngestionBlockingCommand]]().withDefault(topics.blockingIngestion(_).endPoint)

  private lazy val writers =
    Map[String, Map[Int, EndPoint[GraphAlteration]]]().withDefault(topics.graphUpdates(_).endPoint())

  override def establishGraph(req: ClientGraphId): F[Status] =
    req match {
      case ClientGraphId(clientId, graphId, _) =>
        for {
          _      <- F.delay(cluster sendAsync EstablishGraph(graphId, clientId))
          status <- ingestion.establishGraph(req)
        } yield status
    }

  override def submitQuery(req: protocol.Query): F[Stream[F, protocol.QueryManagement]] =
    (req match {
      case protocol.Query(TryQuery(Success(query)), _) => submitDeserializedQuery(query)
      case protocol.Query(TryQuery(Failure(error)), _) => Stream(JobFailed(error))
    }).map(protocol.QueryManagement(_)).pure[F]

  private def submitDeserializedQuery(query: Query): Stream[F, QueryManagement] =
    for {
      _          <- Stream.eval(F.blocking(submissions(query.graphID) sendAsync query))
      queue      <- Stream.eval(Queue.unbounded[F, Option[QueryManagement]])
      dispatcher <- Stream.resource(Dispatcher[F])
      _          <- Stream.resource(QueryTrackerForwarder[F](query.graphID, query.name, topics, queue, dispatcher, config))
      response   <- Stream.fromQueueNoneTerminated(queue, 1000)
    } yield response

  override def submitSource(req: protocol.IngestData): F[Status] = ingestion.ingestData(req)

  override def disconnect(req: ClientGraphId): F[Status] =
    req match {
      case ClientGraphId(clientId, graphId, _) =>
        for {
          _      <- F.delay(cluster sendAsync ClientDisconnected(graphId, clientId))
          status <- ingestion.disconnectClient(req)
        } yield status
    }

  override def destroyGraph(req: protocol.DestroyGraph): F[Status] =
    req match {
      case protocol.DestroyGraph(clientId, graphId, force, _) =>
        for {
          _      <- F.delay(cluster sendAsync DestroyGraph(graphId, clientId, force))
          status <- ingestion.destroyGraph(req)
        } yield status
    }

  override def getNextAvailableId(req: IdPool): F[OptionalId] =
    req match {
      case IdPool(pool, _) => F.blocking(idManager.getNextAvailableID(pool)).map(protocol.OptionalId(_))
    }

  override def processUpdate(req: protocol.GraphUpdate): F[Status] =
    req match {
      case protocol.GraphUpdate(graphId, update, _) =>
        for {
          _ <- F.delay(logger.debug(s"Processing graph update $update"))
          _ <- F.delay(writers(graphId)(partitioner.getPartitionForId(update.srcId)) sendAsync update)
        } yield success
    }

  override def unblockIngestion(req: protocol.UnblockIngestion): F[Status] =
    req match {
      case protocol.UnblockIngestion(graphId, sourceId, messageCount, highestTimeSeen, force, _) =>
        for {
          _       <- F.delay(logger.debug(s"Unblocking ingestion for source id: '$sourceId' and graph id '$graphId'"))
          message <- F.pure(UnblockIngestion(sourceId, graphID = graphId, messageCount, highestTimeSeen, force))
          _       <- F.delay(blockingIngestion(graphId) sendAsync message)
        } yield success
    }

  override def getGraph(req: GetGraph): F[Status] = ingestion.getGraph(req)
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

  private def createService[F[_]](cluster: Resource[F, ServiceRepository[F]], config: Config)(implicit F: Async[F]) =
    for {
      repo            <- cluster
      sourceIDManager <- makeLocalIdManager[F]
      ingestion       <- repo.ingestion
      service         <-
        Resource.eval(Async[F].delay(new DefaultRaphtoryService(ingestion, sourceIDManager, repo.topics, config)))
    } yield service

  private def localCluster[F[_]: Async](config: Config): Resource[F, ServiceRepository[F]] =
    for {
      topics             <- LocalTopicRepository[F](config, None)
      partitionIdManager <- makeLocalIdManager[F]
      serviceRepo        <- LocalServiceRepository(topics)
      _                  <- IngestionServiceInstance(serviceRepo, config)
      _                  <- PartitionOrchestrator[F](config, topics, partitionIdManager)
      _                  <- QueryOrchestrator[F](config, topics)
    } yield serviceRepo

  private def localArrowCluster[F[_]: Async, V: VertexSchema, E: EdgeSchema](
      config: Config
  ): Resource[F, ServiceRepository[F]] =
    for {
      topics             <- LocalTopicRepository[F](config, None)
      partitionIdManager <- makeLocalIdManager[F]
      serviceRepo        <- LocalServiceRepository(topics)
      _                  <- IngestionServiceInstance(serviceRepo, config)
      _                  <- PartitionOrchestrator.applyArrow[V, E, F](config, topics, partitionIdManager)
      _                  <- QueryOrchestrator[F](config, topics)
    } yield serviceRepo

  private def remoteCluster[F[_]: Async](config: Config): Resource[F, ServiceRepository[F]] =
    for {
      topics <- DistributedTopicRepository[F](AkkaConnector.SeedMode, config, None)
      repo   <- DistributedServiceRepository(topics, config)
    } yield repo

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

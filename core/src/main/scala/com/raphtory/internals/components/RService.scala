package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.protobuf.ByteString
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.Topic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.cluster.ClusterManager
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.IngestionBlockingCommand
import com.raphtory.internals.components.querymanager.JobFailed
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.components.querymanager.Submission
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
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.protocol
import com.raphtory.protocol.ClientGraphId
import com.raphtory.protocol.ProtoRaphtoryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import fs2.Stream
import higherkindness.mu.rpc.ChannelFor
import higherkindness.mu.rpc.ChannelForAddress
import org.slf4j.LoggerFactory

trait RaphtoryService[F[_]] {
  def establishGraph(clientID: String, graphID: String): F[Boolean]
  def submitQuery(query: Query): F[Stream[F, QueryManagement]]
  def submitSource(ingestData: IngestData): F[Boolean]
  def disconnect(clientID: String, graphID: String): F[Boolean]
  def destroyGraph(clientID: String, graphID: String, force: Boolean): F[Boolean]
  def getNextAvailableId(pool: String): F[Option[Int]]
  def processUpdate(graphID: String, update: GraphUpdate): F[Boolean]

  def unblockIngestion(
      graphID: String,
      sourceID: Int,
      messageCount: Long,
      highestTimeSeen: Long,
      force: Boolean
  ): F[Boolean]
}

class DefaultRaphtoryService[F[_]](idManager: IDManager, repo: TopicRepository, config: Config)(implicit F: Async[F])
        extends RaphtoryService[F] {
  private val partitioner = new Partitioner(config)

  private lazy val graphSetup = repo.graphSetup.endPoint

  // This is to cache endpoints instead of creating one for every message we send
  private lazy val submissions = Map[String, EndPoint[Submission]]().withDefault(repo.submissions(_).endPoint)
  private lazy val ingestion   = Map[String, EndPoint[IngestData]]().withDefault(repo.ingestSetup(_).endPoint)

  private lazy val blockingIngestion =
    Map[String, EndPoint[IngestionBlockingCommand]]().withDefault(repo.blockingIngestion(_).endPoint)

  private lazy val writers =
    Map[String, Map[Int, EndPoint[GraphAlteration]]]().withDefault(repo.graphUpdates(_).endPoint())

  override def establishGraph(clientID: String, graphID: String): F[Unit] =
    F.delay(graphSetup sendAsync EstablishGraph(graphID, clientID)).as(true)

  override def submitQuery(query: Query): F[Stream[F, QueryManagement]] =
    (for {
      _          <- Stream.eval(F.blocking(submissions(query.graphID) sendAsync query))
      queue      <- Stream.eval(Queue.unbounded[F, Option[QueryManagement]])
      dispatcher <- Stream.resource(Dispatcher[F])
      _          <- Stream.resource(QueryTrackerForwarder[F](query.graphID, query.name, repo, queue, dispatcher, config))
      responses  <- Stream.fromQueueNoneTerminated(queue, 1000)
    } yield responses).pure[F]

  override def submitSource(ingestData: IngestData): F[Boolean] =
    F.delay(ingestion(ingestData.graphID) sendAsync ingestData).as(true)

  override def disconnect(clientID: String, graphID: String): F[Unit] =
    F.delay(graphSetup sendAsync ClientDisconnected(graphID, clientID))

  override def destroyGraph(clientID: String, graphID: String, force: Boolean): F[Unit] =
    F.delay(graphSetup sendAsync DestroyGraph(graphID, clientID, force))

  override def getNextAvailableId(pool: String): F[Option[Int]] = F.blocking(idManager.getNextAvailableID(pool))

  override def processUpdate(graphID: String, update: GraphUpdate): F[Unit] =
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
      _       <- F.delay(blockingIngestion(graphID) sendAsync message)
    } yield ()
}

object RaphtoryService {

  def remote[F[_]: Async](address: String, port: Int): Resource[F, RaphtoryService[F]] = {
    val channelFor: ChannelFor                          = ChannelForAddress(address, port)
    val clientResource: Resource[F, RaphtoryService[F]] = ProtoRaphtoryService.client[F](channelFor)

  }

  private def createService[F[_]](client: ProtoRaphtoryService[F]) =
    new RaphtoryService[F] {

      override def establishGraph(clientID: String, graphID: String): F[Unit] =
        client.establishGraph(ClientGraphId(clientID, graphID))
      override def submitQuery(query: Query): F[Stream[F, QueryManagement]]   = ???
      override def submitSource(ingestData: IngestData): F[Unit]              = ???

      override def disconnect(clientID: String, graphID: String): F[Unit] = ???

      override def destroyGraph(clientID: String, graphID: String, force: Boolean): F[Unit] = ???

      override def getNextAvailableId(pool: String): F[Option[Int]] = ???

      override def processUpdate(graphID: String, update: GraphUpdate): F[Unit] = ???

      override def unblockIngestion(
          graphID: String,
          sourceID: Int,
          messageCount: Long,
          highestTimeSeen: Long,
          force: Boolean
      ): F[Unit] = ???
    }

  def local[F[_]](config: Config)(implicit F: Async[F]): Resource[F, RaphtoryService[F]] =
    for {
      repo               <- LocalTopicRepository[F](config, None)
      partitionIdManager <- makeLocalIdManager[F]
      _                  <- IngestionOrchestrator[F](config, repo)
      _                  <- PartitionOrchestrator[F](config, repo, partitionIdManager)
      _                  <- QueryOrchestrator[F](config, repo)
      sourceIDManager    <- makeLocalIdManager[F]
      service            <- Resource.eval(F.delay(new DefaultRaphtoryService(sourceIDManager, repo, config)))
    } yield service
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

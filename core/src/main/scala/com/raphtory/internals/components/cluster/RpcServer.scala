package com.raphtory.internals.components.cluster

import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.syntax.all._
import com.google.protobuf.ByteString
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.QueryTrackerForwarder.log
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.IngestionBlockingCommand
import com.raphtory.internals.components.querymanager.JobDone
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.Submission
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.protocol.RpcRequest
import com.raphtory.protocol.RpcResponse
import com.raphtory.protocol.RaphtoryService
import com.raphtory.protocol.RpcStatus
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import higherkindness.mu.rpc.server._
import org.slf4j.LoggerFactory

class RpcServer[F[_]](repo: TopicRepository, config: Config)(implicit F: Async[F]) extends RaphtoryService[F] {
  private lazy val kryo       = KryoSerialiser()
  private lazy val graphSetup = repo.graphSetup.endPoint
  private val log: Logger     = Logger(LoggerFactory.getLogger(this.getClass))

  override def processRequest(req: RpcRequest): F[RpcStatus] = {
    log.debug(s"Processing request: $req")
    for {
      message <- F.delay(kryo.deserialise[QueryManagement](req.message.toByteArray))
      _       <- F.delay {
                   message match {
                     case message: Submission               => repo.submissions(message.graphID).endPoint sendAsync message
                     case message: ClusterManagement        => graphSetup sendAsync message
                     case message: IngestionBlockingCommand =>
                       repo.blockingIngestion(message.graphID).endPoint sendAsync message
                   }
                 }
      status  <- F.delay(RpcStatus(true))
    } yield status
  }

  override def submitQuery(req: RpcRequest): F[fs2.Stream[F, RpcResponse]] = {
    log.debug(s"Submitting query: $req")
    val query = kryo.deserialise[Query](req.message.toByteArray)
    for {
      _          <- F.delay(repo.submissions(query.graphID).endPoint sendAsync query)
      queue      <- Queue.unbounded[F, Option[QueryManagement]]
      dispatcher <- Dispatcher[F].allocated
      listener   <- QueryTrackerForwarder[F](query.name, repo, queue, dispatcher._1, config).allocated
      stream     <- F.delay(
                            fs2.Stream
                              .fromQueueNoneTerminated(queue, 1000)
                              .map(message => RpcResponse(ByteString.copyFrom(kryo.serialise(message))))
                              .onFinalize {
                                listener._2 >> dispatcher._2 // release resources
                              }
                    )
    } yield stream
  }
}

object RpcServer {

  def apply[F[_]](repo: TopicRepository, config: Config)(implicit F: Async[F]): Resource[F, Unit] = {
    implicit val rpcServer: RpcServer[F] = new RpcServer(repo, config)
    val portNumber                       = config.getInt("raphtory.deploy.port")
    for {
      serviceDef <- RaphtoryService.bindService[F]
      server     <- Resource.eval(GrpcServer.default[F](portNumber, List(AddService(serviceDef))))
      _          <- GrpcServer.serverResource[F](server)
    } yield ()
  }
}

class QueryTrackerForwarder[F[_]](queue: Queue[F, Option[QueryManagement]], dispatcher: Dispatcher[F], config: Config)
        extends Component[QueryManagement](config) {

  override private[raphtory] def run(): Unit = log.debug("Running QueryTrackerForwarder")

  override def handleMessage(msg: QueryManagement): Unit = {
    log.trace(s"Putting message $msg into the queue")
    dispatcher.unsafeRunSync(queue.offer(Some(msg)))
    if (msg == JobDone)
      dispatcher.unsafeRunSync(queue.offer(None))
  }
  override private[raphtory] def stop(): Unit = log.debug("QueryTrackerListener stopping")
}

object QueryTrackerForwarder {

  private val log: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]](
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
              Seq(repo.queryTrack(jobId)),
              new QueryTrackerForwarder(queue, dispatcher, config)
      )
      .map { component =>
        log.debug(s"Created QueryTrackerForwarder for job: $jobId")
        component
      }
}

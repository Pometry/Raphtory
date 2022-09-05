package com.raphtory.internals.management

import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.protocol.RpcRequest
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import higherkindness.mu.rpc.ChannelFor
import higherkindness.mu.rpc.ChannelForAddress
import org.slf4j.LoggerFactory

class RpcClient[F[_]](dispatcher: Dispatcher[F], repo: TopicRepository, config: Config)(implicit F: Async[F])
        extends Component[QueryManagement](config) {

  private lazy val kryo       = KryoSerialiser()
  private lazy val portNumber = config.getInt("raphtory.deploy.port")
  private lazy val address    = config.getString("raphtory.deploy.address")
  private val log: Logger     = Logger(LoggerFactory.getLogger(this.getClass))

  override private[raphtory] def run(): Unit = log.debug("Running RPC client")

  override def handleMessage(msg: QueryManagement): Unit = {
    val channelFor: ChannelFor                          = ChannelForAddress(address, portNumber)
    val clientResource: Resource[F, RaphtoryService[F]] = RaphtoryService.client[F](channelFor)

    val requestProcessing = clientResource
      .use { service =>
        val encodedMessage = kryo.serialise(msg)
        msg match {
          case query: Query =>
            val queryTrack = repo.queryTrack(query.name).endPoint
            for {
              responses <- service.submitQuery(RpcRequest(encodedMessage))
              _         <- responses
                             .evalTap(response => F.delay(queryTrack sendAsync deserialise(response.message)))
                             .compile
                             .drain
            } yield ()
          case message      =>
            service
              .processRequest(RpcRequest(encodedMessage))
              .map(_ => log.debug(s"Message: '$msg' successfully processed by the server"))
        }
      }

    dispatcher.unsafeRunAndForget(requestProcessing)
  }

  private def deserialise(bytes: Array[Byte]): QueryManagement = kryo.deserialise[QueryManagement](bytes)
}

object RpcClient {

  def apply[F[_]](repo: TopicRepository, config: Config)(implicit F: Async[F]): Resource[F, RpcClient[F]] = {
    val topics = List(repo.graphSetup, repo.submissions(), repo.blockingIngestion())
    for {
      dispatcher <- Dispatcher[F]
      client     <- Component.makeAndStart(repo, "rpc-client", topics, new RpcClient(dispatcher, repo, config))
    } yield client
  }
}

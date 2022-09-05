package com.raphtory.internals.components

import cats.syntax.all._
import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.syntax.spawn._
import com.comcast.ip4s.IpLiteralSyntax
import com.comcast.ip4s.Port
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component.log
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.Submission
import com.raphtory.internals.serialisers.KryoSerialiser
import com.typesafe.config.Config
import fs2.concurrent.Topic
import org.http4s.HttpApp
import org.http4s.HttpRoutes
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.WebSocketFrame
import scodec.bits.ByteVector

class WebServer[F[_]: Spawn](repo: TopicRepository, config: Config)(implicit F: Async[F]) extends Http4sDsl[F] {

  private lazy val kryo       = KryoSerialiser()
  private lazy val portNumber = config.getInt("raphtory.deploy.port")

  private val submissions = repo.submissions.endPoint
  private val graphSetup  = repo.graphSetup.endPoint

  def createServer(): Resource[F, Server] =
    for {
      port   <- Resource.eval(F.pure(Port.fromInt(portNumber).get))
      server <- EmberServerBuilder
                  .default[F]
                  .withHost(ipv4"0.0.0.0")
                  .withPort(port)
                  .withHttpWebSocketApp(createHttpApp)
                  .build
    } yield server

  private def createHttpApp(wsBuilder: WebSocketBuilder2[F]): HttpApp[F] =
    HttpRoutes
      .of[F] {
        case GET -> Root                        =>
          Ok()
        case request @ POST -> Root / "request" =>
          for {
            body     <- request.as[Array[Byte]]
            message  <- F.delay(kryo.deserialise[QueryManagement](body))
            _        <- F.delay {
                          message match {
                            case message: Submission        => submissions sendAsync message
                            case message: ClusterManagement => graphSetup sendAsync message
                          }
                        }
            response <- message match {
                          case query: Query => createQueryWebSocket(query, wsBuilder)
                          case _            => Ok()
                        }
          } yield response
      }
      .orNotFound

  private def createQueryWebSocket(query: Query, wsBuilder: WebSocketBuilder2[F]): F[Response[F]] = {
    val topic = for {
      // FIXME: Why do I need to specify F below? Is it not implicit in this context?
      topic <- Resource.eval(Topic[F, QueryManagement](F))
      _     <- QueryTrackerListener(query, repo, topic)
    } yield topic

    val ignoredClientInput = (stream: fs2.Stream[F, WebSocketFrame]) => fs2.Stream.empty

    topic.use { topic =>
      val responses = topic.subscribe(1000).map(message => WebSocketFrame.Binary(ByteVector(kryo.serialise(message))))
      wsBuilder.build(responses, ignoredClientInput)
    }
  }
}

object WebServer {

  def apply[F[_]: Spawn](repo: TopicRepository, config: Config)(implicit F: Async[F]): Resource[F, Server] =
    new WebServer[F](repo, config).createServer()
}

object QueryTrackerListener {

  def handleMessage[F[_]](topic: Topic[F, QueryManagement], message: QueryManagement): Unit =
    topic.publish1(message)

  // This is a copy paste with some changes from Component.makeAndStart
  def apply[F[_]: Spawn](query: Query, repo: TopicRepository, topic: Topic[F, QueryManagement])(implicit
      F: Async[F]
  ): Resource[F, Unit] = {
    val jobId                = query.name
    val messageHandler       = (message: QueryManagement) => handleMessage(topic, message)
    val listenerDebugMessage =
      s"Started listener.start() fiber for QueryTrackerListener with name [$jobId-web-query-tracker]"
    Resource
      .make {
        for {
          listener    <-
            F.delay(repo.registerListener(s"$jobId-web-query-tracker", messageHandler, repo.queryTrack(jobId)))
          listenerFib <- F.blocking(listener.start())
                           .start
                           .flatTap(_ => F.delay(log.debug(listenerDebugMessage)))
        } yield (listener, listenerFib)
      } {
        case (listener, listenerFib) =>
          for {
            _ <- F.blocking(listener.close())
            _ <- listenerFib.cancel
          } yield ()
      }
      .map { case (_, _) => () }
  }
}

package com.raphtory.internals.management

import cats.syntax.all._
import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.syntax.spawn._
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.serialisers.KryoSerialiser
import com.typesafe.config.Config
import org.http4s.Method
import org.http4s.Request
import org.http4s.Response
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder

class WebClient(repo: TopicRepository, config: Config) extends Component[QueryManagement](config) {

  private lazy val kryo       = KryoSerialiser()
  private lazy val portNumber = config.getInt("raphtory.deploy.port")
  private lazy val address    = config.getInt("raphtory.deploy.address")

  override private[raphtory] def run(): Unit = {} // TODO: log something

  override def handleMessage(msg: QueryManagement): Unit = {
    val clientRequestProcessing = EmberClientBuilder.default[IO].build.use { client =>
      // FIXME: handle the uri exception properly
      val uri            = Uri.fromString(s"http://$address:$portNumber/request").getOrElse(throw new Exception())
      val encodedMessage = kryo.serialise(msg)
      val messageStream  = fs2.Stream.fromIterator[IO](encodedMessage.iterator, encodedMessage.length)
      val request        = Request(method = Method.POST, uri = uri, body = messageStream)

      msg match {
        case query: Query => toQueryTracker(query.name, client.stream(request))
        case _            => client.expect[Array[Byte]](request)
      }
    }

    clientRequestProcessing.unsafeRunSync()
  }

  private def toQueryTracker(jobId: String, stream: fs2.Stream[IO, Response[IO]]): IO[Unit] = {
    val queryTrack = repo.queryTrack(jobId).endPoint
    stream
      .evalTap(response =>
        for {
          body    <- response.as[Array[Byte]]
          message <- IO.delay(kryo.deserialise[QueryManagement](body))
          _       <- IO.delay(queryTrack sendAsync message)
        } yield ()
      )
      .compile
      .drain
  }
}

object WebClient {

  def apply(repo: TopicRepository, config: Config): Resource[IO, WebClient] = {
    val topics = List(repo.graphSetup, repo.submissions)
    Component.makeAndStart(repo, "web-client", topics, new WebClient(repo, config))
  }
}

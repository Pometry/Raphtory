package com.raphtory.internals.management

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.sun.net.httpserver.HttpServer
import com.typesafe.scalalogging.Logger
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.HTTPServer
import org.slf4j.LoggerFactory

import java.net.BindException
import java.net.InetSocketAddress

class Prometheus[IO[_]: Sync](server: HTTPServer) {
  def server: IO[HTTPServer] = Sync[IO].pure(server)
}

object Prometheus {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[IO[_]](port: Int)(implicit IO: Sync[IO]): Resource[IO, Prometheus[IO]] =
    for {
      httpServer <- Resource.make(startHttpServer(port, IO))(server => IO.delay(server.stop(Int.MaxValue)))
      pro        <- Resource.fromAutoCloseable(
                            IO.blocking {
                              val builder = new HTTPServer.Builder()
                                .withHttpServer(httpServer)
                                .withRegistry(CollectorRegistry.defaultRegistry)
                                .withDaemonThreads(true)
                              builder.build()
                            }
                    )
    } yield new Prometheus(pro)

  private def startHttpServer[IO[_]: Sync](port: Int, IO: Sync[IO]) = {
    def innerStart(port: Int) =
      IO.blocking(HttpServer.create(new InetSocketAddress(port), 3))
        .flatTap(s => IO.blocking(logger.info(s"Prometheus started on port ${s.getAddress}")))
    innerStart(port).handleErrorWith {
      case _: BindException =>
        IO.blocking(logger.error(s"Failed to start Prometheus on port $port")) *> innerStart(0)
      case t                => Sync[IO].raiseError(t)
    }
  }
}

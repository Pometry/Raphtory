package com.raphtory.internals.management

import cats.effect.Resource
import cats.effect.Sync
import com.sun.net.httpserver.HttpServer
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.HTTPServer

import java.net.InetSocketAddress

class Prometheus[IO[_]: Sync](server: HTTPServer) {
  def server: IO[HTTPServer] = Sync[IO].pure(server)
}

object Prometheus {

  def apply[IO[_]](port: Int)(implicit IO: Sync[IO]): Resource[IO, Prometheus[IO]] =
    for {
      httpServer <- Resource.make(IO.blocking(HttpServer.create(new InetSocketAddress(port), 3)))(server =>
                      IO.delay(server.stop(Int.MaxValue))
                    )
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
}

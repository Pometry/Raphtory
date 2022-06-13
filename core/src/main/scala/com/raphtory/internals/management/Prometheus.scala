package com.raphtory.internals.management

import cats.effect.Resource
import cats.effect.Sync
import io.prometheus.client.exporter.HTTPServer

class Prometheus[IO[_]: Sync](server: HTTPServer) {
  def server: IO[HTTPServer] = Sync[IO].pure(server)
}

object Prometheus {

  def apply[IO[_]](port: Int)(implicit IO: Sync[IO]): Resource[IO, Prometheus[IO]] =
    Resource
      .fromAutoCloseable(IO.blocking(new HTTPServer(port)))
      .map(pServer => new Prometheus[IO](pServer))
}

package com.raphtory.internals.context

import cats.effect.IO
import com.raphtory.internals.management.QuerySender
import com.typesafe.config.Config

case class Service(client: QuerySender, graphID: String, conf: Config, shutdown: IO[Unit])

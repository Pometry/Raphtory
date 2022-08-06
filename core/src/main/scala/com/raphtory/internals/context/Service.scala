package com.raphtory.internals.context

import cats.effect.IO
import com.raphtory.internals.management.QuerySender

case class Service(client: QuerySender, graphID: String, shutdown: IO[Unit])

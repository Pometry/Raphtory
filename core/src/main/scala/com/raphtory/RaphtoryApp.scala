package com.raphtory

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RaphtoryContext.RaphtoryContextBuilder
import com.raphtory.protocol.RaphtoryService

trait RaphtoryApp {

  final def main(args: Array[String]): Unit = {
    val (serviceAsResource, ctx) = buildContext(RaphtoryContextBuilder())

    val out = for {
      service <- serviceAsResource
    } yield (service, ctx)

    out
      .use {
        case (_, ctx) => IO.blocking(run(ctx))
      }
      .unsafeRunSync()
  }

  def buildContext(ctxBuilder: RaphtoryContextBuilder): (Resource[IO, RaphtoryService[IO]], RaphtoryContext)

  def run(ctx: RaphtoryContext): Unit

}

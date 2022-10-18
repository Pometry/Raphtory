package com.raphtory

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RaphtoryContext.RaphtoryContextBuilder

trait RaphtoryApp {

  final def main(args: Array[String]): Unit = {
    val out = for {
      ctxBuilder <- Resource.fromAutoCloseable(IO.delay(RaphtoryContextBuilder()))
    } yield buildContext(ctxBuilder)

    out
      .use(ctx => IO.blocking(run(ctx)))
      .unsafeRunSync()
  }

  def buildContext(ctxBuilder: RaphtoryContextBuilder): RaphtoryContext

  def run(ctx: RaphtoryContext): Unit
}

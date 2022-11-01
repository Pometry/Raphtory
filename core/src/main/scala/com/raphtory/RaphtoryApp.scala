package com.raphtory

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RaphtoryIOContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema

sealed trait RaphtoryApp {
  def main(args: Array[String]): Unit
  def run(args: Array[String], ctx: RaphtoryContext): Unit
}

object RaphtoryApp {

  abstract class Local extends RaphtoryApp {

    final def main(args: Array[String]): Unit =
      RaphtoryIOContext
        .localIO()
        .use(ctx => IO.blocking(run(args, ctx)))
        .unsafeRunSync()
  }

  abstract class ArrowLocal[V: VertexSchema, E: EdgeSchema] extends RaphtoryApp {

    final def main(args: Array[String]): Unit =
      RaphtoryServiceBuilder
        .arrowStandalone[V, E, IO](defaultConf)
        .use { standalone =>
          IO(run(args, new RaphtoryContext(Resource.eval(IO(standalone)), defaultConf)))
        }
        .unsafeRunSync()
  }

  abstract class Remote(host: String = deployInterface, port: Int = deployPort) extends RaphtoryApp {

    final def main(args: Array[String]): Unit =
      RaphtoryIOContext.remoteIO(host, port).use(ctx => IO.blocking(run(args, ctx))).unsafeRunSync()
  }
}

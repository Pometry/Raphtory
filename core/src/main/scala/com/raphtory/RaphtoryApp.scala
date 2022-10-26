package com.raphtory

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

sealed trait RaphtoryApp {
  def main(args: Array[String]): Unit
  def run(args: Array[String], ctx: RaphtoryContext): Unit
}

object RaphtoryApp {

  abstract class Local extends RaphtoryApp {

    final def main(args: Array[String]): Unit =
      RaphtoryServiceBuilder
        .standalone[IO](defaultConf)
        .use { standalone =>
          IO(run(args, new RaphtoryContext(Resource.eval(IO(standalone)), defaultConf)))
        }
        .unsafeRunSync()
  }

  abstract class Remote(host: String = deployInterface, port: Int = deployPort) extends RaphtoryApp {

    final def main(args: Array[String]): Unit = {
      val config =
        ConfigBuilder()
          .addConfig("raphtory.deploy.address", host)
          .addConfig("raphtory.deploy.port", port)
          .build()
          .getConfig

      val service = RaphtoryServiceBuilder.client[IO](config)
      run(args, new RaphtoryContext(service, config))
    }
  }
}

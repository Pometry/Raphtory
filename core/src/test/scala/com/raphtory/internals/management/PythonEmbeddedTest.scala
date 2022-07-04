package com.raphtory.internals.management

import cats.effect.IO
import munit.CatsEffectSuite

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

class PythonEmbeddedTest extends CatsEffectSuite {
  test("can startup an embedded Python") {
    PythonEmbedded[IO](
            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory")
    ).use { py =>
      for {
        ref <- py.loadGraphBuilder("BaseBuilder", "builder")
        b   <- PythonGraphBuilder(ref, py)
        whaa <- b.parseTuple("Bob")
        _ <- IO.println(whaa)
      } yield  ()

    }
  }

  override def munitTimeout: Duration = FiniteDuration(1, TimeUnit.DAYS)
}

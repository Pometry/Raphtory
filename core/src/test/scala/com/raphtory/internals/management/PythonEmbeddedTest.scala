package com.raphtory.internals.management

import cats.effect.IO
import com.raphtory.api.input.{ImmutableProperty, Properties, Type}
import com.raphtory.internals.graph.GraphAlteration.{EdgeAdd, VertexAdd}
import munit.CatsEffectSuite

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

class PythonEmbeddedTest extends CatsEffectSuite {
  test("can startup an embedded Python") {
    PythonEmbedded[IO](
            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory"),
            Paths.get("/home/murariuf/.virtualenvs/raphtory/lib/python3.8/site-packages")
    ).use { py =>
      for {
        b      <- py.loadGraphBuilder("BaseBuilder", "builder")
        actual <- b.parseTuple("Frodo,Sam,32666")
        _ <- py.javaInterop(() => {
          println("HELLO CALLABLE!")
          "blerg"
        })
      } yield assertEquals(
              actual,
              Vector(
                      VertexAdd(
                              32666,
                              3491048503859410748L,
                              Properties(ImmutableProperty("name", "Frodo")),
                              Some(Type("Character"))
                      ),
                      VertexAdd(
                              32666,
                              -1395500071931009564L,
                              Properties(ImmutableProperty("name", "Sam")),
                              Some(Type("Character"))
                      ),
                      EdgeAdd(
                              32666,
                              3491048503859410748L,
                              -1395500071931009564L,
                              Properties(),
                              Some(Type("Character Co-occurence"))
                      )
              )
      )
    }
  }

  override def munitTimeout: Duration = FiniteDuration(1, TimeUnit.DAYS)
}

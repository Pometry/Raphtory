package com.raphtory.internals.components

import cats.effect.IO
import com.raphtory._
import com.raphtory.protocol.GetGraph
import com.raphtory.protocol.GraphInfo
import com.raphtory.protocol.RaphtoryService
import com.raphtory.protocol.Status
import munit.CatsEffectSuite

class RaphtoryServiceTestSuite extends CatsEffectSuite {

  val f: Fixture[RaphtoryService[IO]] = ResourceSuiteLocalFixture(
          "standalone",
          RaphtoryServiceBuilder.standalone[IO](defaultConf)
  )

  override def munitFixtures: Seq[Fixture[_]] = List(f)

  test("Validate that a graph gets established successfully with a given graphId") {
    val standalone = f()
    val clientId   = createName
    val graphId    = createName

    standalone
      .establishGraph(GraphInfo(clientId, graphId))
      .map { status =>
        assertEquals(status.success, true)
      }
  }

  test("Validate that a graph doesn't exists with a given graphId if the graph is not established already") {
    val standalone = f()
    val graphId    = createName
    standalone.getGraph(GetGraph(graphId)).map(res => assertEquals(res, Status()))
  }

  test("Validate that a graph exists with a given graphId if the graph is established already") {
    val standalone = f()
    val clientId   = createName
    val graphId    = createName
    standalone
      .establishGraph(GraphInfo(clientId, graphId))
      .flatMap { status =>
        if (status.success) standalone.getGraph(GetGraph(graphId)) else IO(Status())
      }
      .map { status =>
        assertEquals(status.success, true)
      }
  }
}

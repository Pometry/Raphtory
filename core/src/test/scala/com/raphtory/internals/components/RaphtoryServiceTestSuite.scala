package com.raphtory.internals.components

import cats.effect.IO
import com.raphtory._
import com.raphtory.protocol._
import munit.CatsEffectSuite

class RaphtoryServiceTestSuite extends CatsEffectSuite {

  case object GraphAlreadyPresent

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
    assertIO(standalone.connectToGraph(GraphInfo(graphId)).handleError(_ => GraphAlreadyPresent), GraphAlreadyPresent)
  }

  test("Validate that a graph exists with a given graphId if the graph is established already") {
    val standalone = f()
    val clientId   = createName
    val graphId    = createName
    for {
      _ <- standalone.establishGraph(GraphInfo(clientId, graphId))
      _ <- assertIO(standalone.connectToGraph(GraphInfo(graphId)), Empty())
    } yield ()
  }
}

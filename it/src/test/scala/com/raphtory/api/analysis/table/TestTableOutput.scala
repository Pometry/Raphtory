package com.raphtory.api.analysis.table

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.time.Perspective
import com.raphtory._
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.graph._
import munit.CatsEffectSuite

class TestTableOutput extends CatsEffectSuite {

  lazy val f: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "graph",
          new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
            .newIOGraph(failOnNotFound = false, destroy = true)
  )

  override def munitFixtures: Seq[Fixture[_]] = List(f)

  test("test table get()") {
    val graph = f()
    graph.addVertex(0, 0)
    graph.addVertex(0, 1)
    graph.addVertex(1, 2)
    graph.addEdge(0, 0, 1)
    graph.addEdge(1, 1, 2)

    val perspectives = graph.range(0, 1, 1).past().execute(EdgeList()).get()
    val result       = perspectives.map(out => out.perspective -> Set.from(out.rows)).toMap

    val correctResult = Map[Perspective, Set[Row]](
            Perspective(0, None, Long.MinValue, 0, formatAsDate = false) -> Set(Row("0", "1")),
            Perspective(1, None, Long.MinValue, 1, formatAsDate = false) -> Set(Row("0", "1"), Row("1", "2"))
    )

    assertEquals(result, correctResult)
  }

}

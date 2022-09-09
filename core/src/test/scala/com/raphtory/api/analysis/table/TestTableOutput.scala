package com.raphtory.api.analysis.table

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.Raphtory
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.time.Perspective
import com.raphtory.internals.graph
import munit.CatsEffectSuite

import scala.collection.mutable

class TestTableOutput extends CatsEffectSuite {

  lazy val suiteGraph: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "graph",
          for {
            g <- Raphtory.newIOGraph()
            _  = g.addVertex(0, 0)
            _  = g.addVertex(0, 1)
            _  = g.addVertex(1, 2)
            _  = g.addEdge(0, 0, 1)
            _  = g.addEdge(1, 1, 2)
          } yield g
  )
  override def munitFixtures                          = List(suiteGraph)

  test("test table get()") {
    val correctResult = Map[Perspective, Set[Row]](
            graph.Perspective(0, None, Long.MinValue, 0) -> Set(Row("0", "1")),
            graph.Perspective(1, None, Long.MinValue, 1) -> Set(Row("0", "1"), Row("1", "2"))
    )
    val g             = suiteGraph()
    val perspectives  = g.range(0, 1, 1).past().execute(EdgeList()).get()
    val result        = perspectives.map(out => out.perspective -> Set.from(out.rows)).toMap
    assertEquals(result, correctResult)
  }

}

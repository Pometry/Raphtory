package com.raphtory.core.algorithm

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.deploy.Raphtory
import com.raphtory.core.time.DiscreteInterval
import org.scalatest.funsuite.AnyFunSuite

class RaphtoryGraphTest extends AnyFunSuite {
  test("Test pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = Raphtory.getGraph()
    val table = graph
      .from(100)
      .until(500)
      .raphtorize(100, 50)
      .filter(_.getState("name") == "some name")
      .step(_.setState("new", 1))
      .transform(ConnectedComponents())
      .select(vertex => Row(vertex.getState("new")))
      .filter(_.getInt(0) == 1)
    val query = table.asInstanceOf[GenericTable].queryBuilder.query

    assert(query.startTime === Some(100))
    assert(query.endTime === Some(500))
    assert(query.increment === Some(DiscreteInterval(100)))
    assert(query.windows === List(DiscreteInterval(50)))

    assert(query.graphFunctions.length === 5)
    assert(query.graphFunctions(0).isInstanceOf[VertexFilter])
    assert(query.graphFunctions(1).isInstanceOf[Step])
    assert(query.graphFunctions(2).isInstanceOf[Step])
    assert(query.graphFunctions(3).isInstanceOf[Iterate])
    assert(query.graphFunctions(4).isInstanceOf[Select])

    assert(query.tableFunctions.length === 1)
    assert(query.tableFunctions.head.isInstanceOf[TableFilter])
  }
}

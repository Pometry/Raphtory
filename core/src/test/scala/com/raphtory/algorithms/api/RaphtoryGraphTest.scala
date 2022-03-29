package com.raphtory.algorithms.api

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.components.querymanager.PointPath
import com.raphtory.deploy.Raphtory
import com.raphtory.time.DiscreteInterval
import org.scalatest.funsuite.AnyFunSuite

class RaphtoryGraphTest extends AnyFunSuite {
  test("Test overall pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = Raphtory.getGraph()
    val table = graph
      .from(100)
      .until(500)
      .walk(100)
      .window(50)
      .filter(_.getState("name") == "some name")
      .step(_.setState("new", 1))
      .transform(ConnectedComponents())
      .select(vertex => Row(vertex.getState("new")))
      .filter(_.getInt(0) == 1)
    val query = table.asInstanceOf[GenericTable].query

    assert(query.timelineStart === 100)
    assert(query.timelineEnd === 500)
    assert(query.points.asInstanceOf[PointPath].increment === DiscreteInterval(100))
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

  test("Test timestamp format without milliseconds") {
    val query = Raphtory
      .getGraph()
      .from("2020-02-25 23:12:08")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with milliseconds") {
    val query = Raphtory
      .getGraph()
      .from("2020-02-25 23:12:08.567")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with date") {
    val query = Raphtory
      .getGraph()
      .from("2020-02-25")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for 2 digit milliseconds") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm:ss[.SS]")
    val query = Raphtory
      .getGraph(conf)
      .from("2020-02-25 23:12:08.56")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for hours and minutes") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm")
    val query = Raphtory
      .getGraph(conf)
      .from("2020-02-25 12:23")
      .query

    assert(query.timelineStart != Long.MinValue)
  }
}

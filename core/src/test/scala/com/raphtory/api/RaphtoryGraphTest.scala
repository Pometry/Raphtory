package com.raphtory.api

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.ClearChain
import com.raphtory.api.analysis.graphview.Iterate
import com.raphtory.api.analysis.graphview.Select
import com.raphtory.api.analysis.graphview.Step
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.TableImplementation
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.time.DiscreteInterval
import org.scalatest.funsuite.AnyFunSuite

class RaphtoryGraphTest extends AnyFunSuite {
  test("Test overall pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = Raphtory.connect()
    val table = graph
      .from(100)
      .until(500)
      .walk(100)
      .window(50)
      .vertexFilter(_.getState[String]("name") == "some name")
      .step(_.setState("new", 1))
      .transform(ConnectedComponents())
      .select(vertex => Row(vertex.getState("new")))
      .filter(_.getInt(0) == 1)
    val query = table.asInstanceOf[TableImplementation].query

    graph.disconnect()

    assert(query.timelineStart === 100)
    assert(query.timelineEnd === 499)
    assert(query.points.asInstanceOf[PointPath].increment === DiscreteInterval(100))
    assert(query.windows === List(DiscreteInterval(50)))

    assert(query.graphFunctions.length === 6)
    assert(query.graphFunctions(0).isInstanceOf[Step])
    assert(query.graphFunctions(1).isInstanceOf[Step])
    assert(query.graphFunctions(2).isInstanceOf[Step])
    assert(query.graphFunctions(3).isInstanceOf[Iterate])
    assert(query.graphFunctions(4).isInstanceOf[ClearChain])
    assert(query.graphFunctions(5).isInstanceOf[Select])

    assert(query.tableFunctions.length === 1)
    assert(query.tableFunctions.head.isInstanceOf[TableFilter])
  }

  test("Test timestamp format without milliseconds") {
    val graph = Raphtory.connect()
    val query = graph
      .from("2020-02-25 23:12:08")
      .query

    graph.disconnect()

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with milliseconds") {
    val graph = Raphtory.connect()
    val query = graph
      .from("2020-02-25 23:12:08.567")
      .query

    graph.disconnect()

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with date") {
    val graph = Raphtory.connect()
    val query = graph
      .from("2020-02-25")
      .query

    graph.disconnect()

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for 2 digit milliseconds") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm:ss[.SS]")
    val graph = Raphtory.connect(conf)
    val query = graph
      .from("2020-02-25 23:12:08.56")
      .query

    graph.disconnect()

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for hours and minutes") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm")
    val graph = Raphtory.connect(conf)
    val query = graph
      .from("2020-02-25 12:23")
      .query

    graph.disconnect()

    assert(query.timelineStart != Long.MinValue)
  }
}

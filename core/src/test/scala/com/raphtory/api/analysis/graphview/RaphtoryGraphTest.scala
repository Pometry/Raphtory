package com.raphtory.api.analysis.graphview

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.TableImplementation
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.typesafe.config.Config
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** This test uses a mock TemporalGraph as starting point
  * to check if the objects within this hierarchy create the Query object correctly.
  * It doesn't deploy or connect to anything.
  */
class RaphtoryGraphTest extends AnyFunSuite {

  private def createMockGraph(config: Map[String, String] = Map()) =
    new TemporalGraph(Query(), null, Raphtory.getDefaultConfig(config))

  test("Test overall pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = createMockGraph()
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
    val graph = createMockGraph()
    val query = graph
      .from("2020-02-25 23:12:08")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with milliseconds") {
    val graph = createMockGraph()
    val query = graph
      .from("2020-02-25 23:12:08.567")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with date") {
    val graph = createMockGraph()
    val query = graph
      .from("2020-02-25")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for 2 digit milliseconds") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm:ss[.SS]")
    val graph = createMockGraph(conf)
    val query = graph
      .from("2020-02-25 23:12:08.56")
      .query

    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for hours and minutes") {
    val conf  = Map("raphtory.query.timeFormat" -> "yyyy-MM-dd HH:mm")
    val graph = createMockGraph(conf)
    val query = graph
      .from("2020-02-25 12:23")
      .query

    assert(query.timelineStart != Long.MinValue)
  }
}

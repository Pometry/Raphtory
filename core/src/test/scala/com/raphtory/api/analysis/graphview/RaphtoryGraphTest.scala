package com.raphtory.api.analysis.graphview

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.TableImplementation
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.typesafe.config.Config
import org.scalatest.BeforeAndAfterAll
import munit.FunSuite

/** This test uses a mock TemporalGraph as starting point
  * to check if the objects within this hierarchy create the Query object correctly.
  * It doesn't deploy or connect to anything.
  */
class RaphtoryGraphTest extends FunSuite {

  private def createMockGraph(config: Config = ConfigBuilder.getDefaultConfig) =
    new TemporalGraph(Query(graphID = ""), null, config)

  test("Test overall pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = createMockGraph()
    val table = graph
      .from(100)
      .until(500)
      .walk(100)
      .window(50)
      .vertexFilter(_.getState[String]("name") == "some name")
      .step(_.setState("new", 1))
      .transform(ConnectedComponents)
      .select(vertex => Row(vertex.getState("new")))
      .filter(_.getInt(0) == 1)
    val query = table.asInstanceOf[TableImplementation].query

    assertEquals(query.timelineStart, 100L)
    assertEquals(query.timelineEnd, 499L)
    assertEquals(query.points.asInstanceOf[PointPath].increment, DiscreteInterval(100))
    assertEquals(query.windows, List(DiscreteInterval(50)))

    assertEquals(query.graphFunctions.length, 6)
    assert(query.graphFunctions(0).isInstanceOf[Step[Vertex]])
    assert(query.graphFunctions(1).isInstanceOf[Step[Vertex]])
    assert(query.graphFunctions(2).isInstanceOf[Step[Vertex]])
    assert(query.graphFunctions(3).isInstanceOf[Iterate[Vertex]])
    assert(query.graphFunctions(4).isInstanceOf[ClearChain])
    assert(query.graphFunctions(5).isInstanceOf[Select[Vertex]])

    assertEquals(query.tableFunctions.length, 1)
    assert(query.tableFunctions.head.isInstanceOf[TableFilter])
  }

  test("Test timestamp format without milliseconds") {
    val graph = createMockGraph()
    val query = graph.from("2020-02-25 23:12:08").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with milliseconds") {
    val graph = createMockGraph()
    val query = graph.from("2020-02-25 23:12:08.567").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with date") {
    val graph = createMockGraph()
    val query = graph.from("2020-02-25").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for 2 digit milliseconds") {
    val graph =
      createMockGraph(
              ConfigBuilder()
                .addConfig("raphtory.query.timeFormat", "yyyy-MM-dd HH:mm:ss[.SS]")
                .build()
                .getConfig
      )
    val query = graph.from("2020-02-25 23:12:08.56").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for hours and minutes") {
    val graph =
      createMockGraph(
              ConfigBuilder()
                .addConfig("raphtory.query.timeFormat", "yyyy-MM-dd HH:mm")
                .build()
                .getConfig
      )
    val query = graph.from("2020-02-25 12:23").query
    assert(query.timelineStart != Long.MinValue)
  }
}

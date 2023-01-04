package com.raphtory.api.analysis.graphview

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.TableImplementation
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.QuerySender
import com.typesafe.config.Config
import munit.FunSuite

/** This test uses a mock TemporalGraph as starting point
  * to check if the objects within this hierarchy create the Query object correctly.
  * It doesn't deploy or connect to anything.
  */
class RaphtoryGraphTest extends FunSuite {

  class DummyQuerySender(config: Config)
          extends QuerySender(
                  graphID = "",
                  service = null,
                  config = config,
                  clientID = ""
          ) {
//    override def IDForUpdates(): Int = 1
  }

  private def createMockGraph(config: Config = ConfigBuilder.getDefaultConfig) =
    new TemporalGraph(Query(graphID = ""), new DummyQuerySender(config), config)

  test("Test overall pipeline syntax for RaphtoryGraph class and related hierarchy") {
    val graph = createMockGraph()
    val table = graph
      .startingFrom(100)
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
    assertEquals(query.windows.toList, List(DiscreteInterval(50)))

    assertEquals(query.operations.length, 7)
    assert(query.operations(0).isInstanceOf[Step[_]])
    assert(query.operations(1).isInstanceOf[Step[_]])
    assert(query.operations(2).isInstanceOf[Step[_]])
    assert(query.operations(3).isInstanceOf[Iterate[_]])
    assert(query.operations(4).isInstanceOf[ClearChain])
    assert(query.operations(5).isInstanceOf[Select[_]])
    assert(query.operations(6).isInstanceOf[TableFilter])
  }

  test("Test timestamp format without milliseconds") {
    val graph = createMockGraph()
    val query = graph.startingFrom("2020-02-25 23:12:08").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with milliseconds") {
    val graph = createMockGraph()
    val query = graph.startingFrom("2020-02-25 23:12:08.567").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with date") {
    val graph = createMockGraph()
    val query = graph.startingFrom("2020-02-25").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for 2 digit milliseconds") {
    val graph =
      createMockGraph(
              ConfigBuilder()
                .build()
                .getConfig
      )
    val query = graph.startingFrom("2020-02-25 23:12:08.56", "yyyy-MM-dd HH:mm:ss.SS").query
    assert(query.timelineStart != Long.MinValue)
  }

  test("Test timestamp format with custom configuration for hours and minutes") {
    val graph =
      createMockGraph(
              ConfigBuilder()
                .build()
                .getConfig
      )
    val query = graph.startingFrom("2020-02-25 12:23").query
    assert(query.timelineStart != Long.MinValue)
  }
}

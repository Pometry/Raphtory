package com.raphtory.api.analysis.graphview

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NeighbourNames
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.input.Spout
import com.raphtory.spouts.SequenceSpout
import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery

import scala.util.Random

class WindowedOutEdgeHistory(after: Long, before: Long) extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    NeighbourNames(graph.reducedView)

  override def tabularise(graph: ReducedGraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex
        .getOutEdges(after, before)
        .flatMap(edge =>
          edge.history().collect {
            case event if event.event =>
              val names: Map[vertex.IDType, String] = vertex.getState("neighbourNames")
              Row(vertex.name(), names(edge.dst), event.time)
          }
        )
    }
}

class WindowedInEdgeHistory(after: Long, before: Long) extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    NeighbourNames(graph.reducedView)

  override def tabularise(graph: ReducedGraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex
        .getInEdges(after, before)
        .flatMap(edge =>
          edge.history().collect {
            case event if event.event =>
              val names: Map[vertex.IDType, String] = vertex.getState("neighbourNames")
              Row(names(edge.src), vertex.name(), event.time)
          }
        )
    }
}

class HistoryTest extends BaseCorrectnessTest(startGraph = true) {
  val rng = new Random(42) // fixed network

  val edges = {
    for (i <- 0 until 100) yield s"${rng.nextInt(20)},${rng.nextInt(20)}"
  }
  val input: Seq[String] = edges.zipWithIndex.map { case (s, i) => s"$s,$i" }

  override def setSpout(): Spout[String] = SequenceSpout(input: _*)

  val res: Seq[String] = edges.distinct.map(e => s"${edges.size - 1},$e")

  val resExploded: Seq[String] = input.map(e => s"${edges.size - 1},$e")

  test("test edge ingestion and output") {
    correctnessTest(TestQuery(EdgeList(), edges.size - 1), res)
  }

  test("test exploded edge output") {
    correctnessTest(TestQuery(TemporalEdgeList(), edges.size - 1), resExploded)
  }

  test("test exploded edge output with window") {
    val query = TestQuery(TemporalEdgeList(), 70, List(30))
    val res   = input
      .filter { e =>
        val t = e.split(",").last.toInt
        t > query.timestamp - query.windows.head && t <= query.timestamp
      }
      .map(e => s"${query.timestamp},${query.windows.head},$e")
    correctnessTest(query, res)
  }

  test("test windowing functionality") {
    correctnessTest(
            TestQuery(EdgeList(), edges.size - 2, List(edges.size - 4)),
            edges
              .slice(
                      2,
                      edges.size - 1
              ) // slice has exclusive end but our window is inclusive
              .distinct
              .map(e => s"${edges.size - 2},${edges.size - 4},$e")
    )
  }

  test("test windowing functionality on undirected view") {
    correctnessTest(
            TestQuery(EdgeList(), edges.size - 2, List(edges.size - 4)),
            graphS.undirectedView,
            edges
              .slice(
                      2,
                      edges.size - 1
              ) // slice has exclusive end but our window is inclusive
              .distinct
              .map(e => s"${edges.size - 2},${edges.size - 4},$e")
              .flatMap { e =>
                val parts = e.split(",")
                List(e, List(parts(0), parts(1), parts(3), parts(2)).mkString(","))
              }
              .distinct
    )
  }

  test("test out-edge history access with time window") {
    val after  = 10
    val before = 40
    val res    = resExploded.filter { e =>
      val t = e.split(",").last.toInt
      t >= after && t <= before
    }
    correctnessTest(
            TestQuery(new WindowedOutEdgeHistory(after, before), edges.size - 1),
            res
    )
  }

  test("test out-edge history access with time window on undirected view") {
    val after  = 10
    val before = 40
    val res    = resExploded
      .filter { e =>
        val t = e.split(",").last.toInt
        t >= after && t <= before
      }
      .flatMap { e =>
        val parts = e.split(",")
        List(e, List(parts(0), parts(2), parts(1), parts(3)).mkString(","))
      }
      .distinct

    correctnessTest(
            TestQuery(new WindowedOutEdgeHistory(after, before), edges.size - 1),
            graphS.undirectedView,
            res
    )
  }

  test("test out-edge history access with time window and restricted view") {
    val after     = 10
    val before    = 40
    val timestamp = 30
    val window    = 10
    val res       = input
      .filter { e =>
        val t = e.split(",").last.toInt
        t >= math.max(after, timestamp - window + 1) && t <= math.min(timestamp, before)
      }
      .map(e => s"$timestamp,$window,$e")
    correctnessTest(
            TestQuery(new WindowedOutEdgeHistory(after, before), timestamp, List(window)),
            res
    )
  }

  test("test out-edge history access with time window and restricted view on undirected view") {
    val after     = 10
    val before    = 40
    val timestamp = 30
    val window    = 10
    val res       = input
      .filter { e =>
        val t = e.split(",").last.toInt
        t >= math.max(after, timestamp - window + 1) && t <= math.min(timestamp, before)
      }
      .map(e => s"$timestamp,$window,$e")
      .flatMap { e =>
        val parts = e.split(",")
        List(e, List(parts(0), parts(1), parts(3), parts(2), parts(4)).mkString(","))
      }
      .distinct
    correctnessTest(
            TestQuery(new WindowedOutEdgeHistory(after, before), timestamp, List(window)),
            graphS.undirectedView,
            res
    )
  }

  test("test in-edge history access with time window") {
    val after  = 10
    val before = 40
    val res    = resExploded.filter { e =>
      val t = e.split(",").last.toInt
      t >= after && t <= before
    }
    correctnessTest(
            TestQuery(new WindowedInEdgeHistory(after, before), edges.size - 1),
            res
    )
  }

  test("test in-edge history access with time window on undirected view") {
    val after  = 10
    val before = 40
    val res    = resExploded
      .filter { e =>
        val t = e.split(",").last.toInt
        t >= after && t <= before
      }
      .flatMap { e =>
        val parts = e.split(",")
        List(e, List(parts(0), parts(2), parts(1), parts(3)).mkString(","))
      }
      .distinct

    correctnessTest(
            TestQuery(new WindowedInEdgeHistory(after, before), edges.size - 1),
            graphS.undirectedView,
            res
    )
  }

  test("test temporal node-list output with window") {
    val query = TestQuery(TemporalNodeList(), 70, List(30))
    val res   = input
      .filter { e =>
        val t = e.split(",").last.toInt
        t > query.timestamp - query.windows.head && t <= query.timestamp
      }
      .flatMap { e =>
        val s = e.split(",")
        List(
                s"${query.timestamp},${query.windows.head},${s.head},${s.last}",
                s"${query.timestamp},${query.windows.head},${s(1)},${s.last}"
        )
      }
      .distinct
    correctnessTest(query, res)
  }

}

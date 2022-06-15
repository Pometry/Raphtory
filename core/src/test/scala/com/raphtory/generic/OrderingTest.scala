package com.raphtory.generic

import com.raphtory.BaseCorrectnessTest
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.generic.CheckHistory.isSortedIncreasing
import com.raphtory.internals.storage.pojograph.OrderedBuffer
import com.raphtory.spouts.SequenceSpout
import OrderedBuffer.HistoricEventOrdering
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source
import scala.util.Random
import scala.math.Ordering.Implicits._
import scala.reflect.ClassTag

class CheckHistory extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { graphState =>
        graphState.newAll("vertexHistoryOrdered")
        graphState.newAll("edgeHistoryOrdered")
      }
      .step { (vertex, graphState) =>
        val history = vertex.history()
        val sorted  = isSortedIncreasing(history)
        vertex.getEdges().foreach { edge =>
          val history = edge.history()
          val sorted  = isSortedIncreasing(history)
          graphState("edgeHistoryOrdered") += sorted
        }
        graphState("vertexHistoryOrdered") += sorted
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(graphState =>
      Row(graphState("vertexHistoryOrdered").value, graphState("edgeHistoryOrdered").value)
    )
}

object CheckHistory {
  def apply() = new CheckHistory

  def isSortedIncreasing[T: Ordering](seq: Seq[T]): Boolean =
    seq match {
      case Seq()  => true
      case Seq(_) => true
      case _      => seq.sliding(2).forall(seq => seq(0) <= seq(1))
    }
}

class OrderingTest extends BaseCorrectnessTest {
  test("test history is sorted") {
    correctnessTest(
            TestQuery(CheckHistory(), 23),
            for (i <- 0 until 100)
              yield s"${Random.nextInt(10)},${Random.nextInt(10)},${Random.nextInt(100)}",
            Seq("23,true,true")
    )
  }
}

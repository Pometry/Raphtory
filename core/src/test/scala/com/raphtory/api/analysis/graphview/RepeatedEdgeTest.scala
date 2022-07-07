package com.raphtory.api.analysis.graphview

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.WeightedGraphBuilder
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.SequenceSpout

import scala.util.Random

class RepeatedEdgeTest extends BaseCorrectnessTest(startGraph = true) {
  val rng = new Random(42) // fixed network

  val edges = {
    for (i <- 0 until 100) yield s"${rng.nextInt(20)},${rng.nextInt(20)}"
  }
  val input: Seq[String] = (0 until 4).flatMap(j => edges.zipWithIndex.map { case (s, i) => s"$s,$i, $j" })

  override def setSpout(): Spout[String] = SequenceSpout(input: _*)

  override def setGraphBuilder(): GraphBuilder[String] = WeightedGraphBuilder()

  test("Multilayer edgelist test with repeated edges") {
    correctnessTest(TestQuery(TemporalEdgeList("weight"), edges.size - 1), input)
  }
}

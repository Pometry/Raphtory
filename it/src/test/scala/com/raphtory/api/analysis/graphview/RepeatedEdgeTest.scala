package com.raphtory.api.analysis.graphview

import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.api.input.Source
import com.raphtory.spouts.SequenceSpout
import com.test.raphtory.WeightedGraphBuilder

import scala.util.Random

class RepeatedEdgeTest extends BaseCorrectnessTest {
  val rng = new Random(42) // fixed network

  val edges = {
    for (i <- 0 until 100) yield s"${rng.nextInt(20)},${rng.nextInt(20)}"
  }
  val input                      = edges.zipWithIndex.map { case (s, i) => s"$s,$i" }
  val repeatedInput: Seq[String] = (0 until 4).flatMap(j => input.map(s => s"$s,$j"))

  test("Multilayer edgelist test with repeated edges") {
    val res = repeatedInput.map(s => s"${edges.size - 1},$s")
    correctnessTest(TestQuery(TemporalEdgeList("weight"), edges.size - 1), res)
  }

  override def setSource(): Source = Source(SequenceSpout(repeatedInput), WeightedGraphBuilder())
}

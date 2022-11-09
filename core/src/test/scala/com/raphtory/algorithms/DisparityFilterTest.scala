package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.filters.DisparityPValues
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.DoubleProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.spouts.ResourceSpout

class DisparityFilterTest extends BaseCorrectnessTest {
  test("Test Disparity Filter") {
    correctnessTest(
            TestQuery(DisparityPValues[Double]() -> EdgeList(Array("pvalue")), 1),
            "DisparityFilter/results.csv"
    )
  }
  
  override def setSource(): Source =
    Source[String](
            ResourceSpout("DisparityFilter/input.csv"),
            (
                (
                    graph,
                    tuple
                ) => {
                  val fields = tuple.split(",")
                  val src    = fields(0).toLong
                  val dst    = fields(1).toLong
                  val weight = fields(2).toDouble
                  val time   = fields(3).toLong
                  graph.addVertex(updateTime = time, src)
                  graph.addVertex(updateTime = time, dst)
                  graph.addEdge(updateTime = time, src, dst, Properties(DoubleProperty("weight", weight)))
                }
            )
    )
}
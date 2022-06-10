package com.raphtory

import com.google.common.hash.Hashing
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

import java.nio.charset.StandardCharsets

abstract class BaseCorrectnessTest extends BaseRaphtoryAlgoTest[String] {

  override def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder()

  def setSpout(): Spout[String] = new IdentitySpout

  private def correctResultsHash(resultsResource: String): String = {
    val source = scala.io.Source.fromResource(resultsResource)
    try resultsHash(source.getLines())
    finally source.close()
  }

  override def beforeAll(): Unit = setup()

  override def afterAll(): Unit = {}

  private def correctResultsHash(rows: IterableOnce[String]): String =
    resultsHash(rows)

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphResource: String,
      resultsResource: String,
      lastTimestamp: Int
  ): Boolean = {
    graph = Raphtory.load(ResourceSpout(graphResource), setGraphBuilder())

    val res = algorithmPointTest(
            algorithm,
            lastTimestamp
    ) == correctResultsHash(
            resultsResource
    )
    graph.deployment.stop()
    res
  }

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphEdges: Seq[String],
      results: Seq[String],
      lastTimestamp: Int
  ): Boolean = {
    graph = Raphtory.load(SequenceSpout(graphEdges: _*), setGraphBuilder())
    val res = algorithmPointTest(
            algorithm,
            lastTimestamp
    ) == correctResultsHash(
            results
    )
    graph.deployment.stop()
    res
  }
}

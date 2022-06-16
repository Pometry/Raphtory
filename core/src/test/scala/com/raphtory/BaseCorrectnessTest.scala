package com.raphtory

import com.google.common.hash.Hashing
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

import java.nio.charset.StandardCharsets

abstract class BaseCorrectnessTest(
    deleteResultAfterFinish: Boolean = true,
    startGraph: Boolean = false
) extends BaseRaphtoryAlgoTest[String](
                deleteResultAfterFinish,
                startGraph
        ) {

  override def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder()

  def setSpout(): Spout[String] = new IdentitySpout

  private def correctResultsHash(resultsResource: String): String = {
    val source = scala.io.Source.fromResource(resultsResource)
    try resultsHash(source.getLines())
    finally source.close()
  }

  private def correctResultsHash(rows: IterableOnce[String]): String =
    resultsHash(rows)

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphResource: String,
      resultsResource: String,
      lastTimestamp: Int
  ): Boolean = {
    graph = Raphtory.load(ResourceSpout(graphResource), setGraphBuilder())
    try correctnessTest(algorithm, resultsResource, lastTimestamp)
    finally graph.deployment.stop()
  }

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphEdges: Seq[String],
      results: Seq[String],
      lastTimestamp: Int
  ): Boolean = {
    graph = Raphtory.load(SequenceSpout(graphEdges: _*), setGraphBuilder())
    try correctnessTest(algorithm, results, lastTimestamp)
    finally graph.deployment.stop()
  }

  def correctnessTest(
      algorithm: GenericallyApplicable,
      results: Seq[String],
      lastTimestamp: Int
  ): Boolean =
    algorithmPointTest(
            algorithm,
            lastTimestamp
    ) == correctResultsHash(
            results
    )

  def correctnessTest(
      algorithm: GenericallyApplicable,
      resultsResource: String,
      lastTimestamp: Int
  ): Boolean =
    algorithmPointTest(
            algorithm,
            lastTimestamp
    ) == correctResultsHash(
            resultsResource
    )
}

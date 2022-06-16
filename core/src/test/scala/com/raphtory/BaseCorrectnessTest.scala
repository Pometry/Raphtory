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

case class TestQuery(
    algorithm: GenericallyApplicable,
    timestamp: Long,
    windows: List[Long] = List.empty[Long]
)

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
      test: TestQuery,
      graphResource: String,
      resultsResource: String
  ): Boolean = {
    graph = Raphtory.load(ResourceSpout(graphResource), setGraphBuilder())
    try correctnessTest(test, resultsResource)
    finally graph.deployment.stop()
  }

  def correctnessTest(
      test: TestQuery,
      graphEdges: Seq[String],
      results: Seq[String]
  ): Boolean = {
    graph = Raphtory.load(SequenceSpout(graphEdges: _*), setGraphBuilder())
    try correctnessTest(test, results)
    finally graph.deployment.stop()
  }

  def correctnessTest(
      test: TestQuery,
      results: Seq[String]
  ): Boolean =
    algorithmPointTest(
            test.algorithm,
            test.timestamp,
            test.windows
    ) == correctResultsHash(
            results
    )

  def correctnessTest(
      test: TestQuery,
      resultsResource: String
  ): Boolean =
    algorithmPointTest(
            test.algorithm,
            test.timestamp,
            test.windows
    ) == correctResultsHash(
            resultsResource
    )
}

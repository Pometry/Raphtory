package com.raphtory

import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.input.{GraphBuilder, Spout}
import com.raphtory.spouts.{IdentitySpout, ResourceSpout, SequenceSpout}

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
  ): Boolean =
    Raphtory
      .load(ResourceSpout(graphResource), setGraphBuilder())
      .use {
        algorithmPointTest(test.algorithm, test.timestamp,test.windows)
      }
      .map(actual =>
        actual == correctResultsHash(
                resultsResource
        )
      )
      .unsafeRunSync()

  def correctnessTest(
      test: TestQuery,
      graphEdges: Seq[String],
      results: Seq[String]
  ): Boolean =
    Raphtory
      .load(SequenceSpout(graphEdges: _*), setGraphBuilder())
      .use {
        algorithmPointTest(
                test.algorithm,
          test.timestamp,
          test.windows
        )
      }
      .map(actual =>
        actual == correctResultsHash(
                results
        )
      )
      .unsafeRunSync()
}

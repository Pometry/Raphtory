package com.raphtory

import cats.effect.IO
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

case class TestQuery(
    algorithm: GenericallyApplicable,
    timestamp: Long,
    windows: List[Long] = List.empty[Long]
)

abstract class BaseCorrectnessTest(
    deleteResultAfterFinish: Boolean = true,
    startGraph: Boolean = false
) extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish) {

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
  ): IO[Unit] =
    Raphtory
      .load(ResourceSpout(graphResource), setGraphBuilder())
      .use { g =>
        algorithmPointTest(test.algorithm, test.timestamp, test.windows, graph = g)
      }
      .map(assertEquals(_, correctResultsHash(resultsResource)))

  def correctnessTest(
      test: TestQuery,
      graphEdges: Seq[String],
      results: Seq[String]
  ): IO[Unit] =
    Raphtory
      .load(SequenceSpout(graphEdges: _*), setGraphBuilder())
      .use { g =>
        algorithmPointTest(
                test.algorithm,
                test.timestamp,
                test.windows,
                graph = g
        )
      }
      .map(assertEquals(_, correctResultsHash(results)))

  def correctnessTest(test: TestQuery, results: Seq[String]): IO[Unit] =
    algorithmPointTest(test.algorithm, test.timestamp, test.windows).map { obtained =>
      val expected = correctResultsHash(results)
      assertEquals(obtained, expected)
    }

  def correctnessTest(test: TestQuery, results: String): IO[Unit] =
    algorithmPointTest(test.algorithm, test.timestamp, test.windows).map { obtained =>
      val expected = correctResultsHash(results)
      assertEquals(obtained, expected)
    }

}

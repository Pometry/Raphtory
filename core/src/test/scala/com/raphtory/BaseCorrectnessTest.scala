package com.raphtory

import cats.effect.IO
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

import scala.collection.mutable

case class TestQuery(
    algorithm: GenericallyApplicable,
    timestamp: Long,
    windows: List[Long] = List.empty[Long]
)

abstract class BaseCorrectnessTest(
    deleteResultAfterFinish: Boolean = true,
    startGraph: Boolean = false
) extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish) {

  private def runTest(test: TestQuery, graph: TemporalGraph = graphS) =
    IO {
      val tracker =
        graph.at(test.timestamp).window(test.windows, Alignment.END).execute(test.algorithm).writeTo(defaultSink)
      tracker.waitForJob()
      getResults(tracker.getJobId)
    }

//  private def normaliseResults(results: IterableOnce[String]): collection.Map[String, Int] = {
//    val map = mutable.Map.empty[String, Int].withDefaultValue(0)
//    results.iterator.foreach(result => map(result) += 1)
//    map
//  }

  private def normaliseResults(value: IterableOnce[String]) =
    value.iterator.toList.sorted.mkString("\n")

  override def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder()

  def setSpout(): Spout[String] = new IdentitySpout

  private def correctResultsHash(resultsResource: String): String = {
    val source = scala.io.Source.fromResource(resultsResource)
    try resultsHash(source.getLines())
    finally source.close()
  }

  private def correctResultsHash(rows: IterableOnce[String]): String =
    resultsHash(rows)

  def assertResultsMatch(obtained: IterableOnce[String], resultsResource: String): Unit = {
    val source = scala.io.Source.fromResource(resultsResource)
    try assertResultsMatch(obtained, source.getLines())
    finally source.close()
  }

  def assertResultsMatch(obtained: IterableOnce[String], results: IterableOnce[String]): Unit =
    assertEquals(normaliseResults(obtained), normaliseResults(results))

  def correctnessTest(
      test: TestQuery,
      graphResource: String,
      resultsResource: String
  ): IO[Unit] =
    Raphtory
      .loadIO(ResourceSpout(graphResource), setGraphBuilder())
      .use { g =>
        runTest(test, graph = g)
      }
      .map(obtained => assertResultsMatch(obtained, resultsResource))

  def correctnessTest(
      test: TestQuery,
      graphEdges: Seq[String],
      results: Seq[String]
  ): IO[Unit] =
    Raphtory
      .loadIO(SequenceSpout(graphEdges: _*), setGraphBuilder())
      .use { g =>
        runTest(test, g)
      }
      .map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, results: Seq[String]): IO[Unit] =
    runTest(test).map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, graph: TemporalGraph, results: Seq[String]): IO[Unit] =
    runTest(test, graph).map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, results: String): IO[Unit] =
    runTest(test).map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, graph: TemporalGraph, results: String): IO[Unit] =
    runTest(test, graph).map(obtained => assertResultsMatch(obtained, results))

}

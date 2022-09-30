package com.raphtory

import cats.effect.IO
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.{CSVEdgeListSource, Graph, GraphBuilder, Source, Spout}
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

import scala.collection.mutable

case class TestQuery(
    algorithm: GenericallyApplicable,
    timestamp: Long = -1,
    windows: List[Long] = List.empty[Long]
)

trait Edges extends Spout[String]

object Edges {
  implicit def edgesFromResource(resource: String)  = new ResourceSpout(resource) with Edges
  implicit def edgesFromEdgeSeq(edges: Seq[String]) = new SequenceSpout(edges) with Edges
}

trait Result

abstract class BaseCorrectnessTest(
    deleteResultAfterFinish: Boolean = true
) extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish) {

  private def runTest(test: TestQuery, graph: TemporalGraph = graphS) =
    IO {
      val tracker = (if (test.timestamp >= 0)
                       graph.at(test.timestamp).window(test.windows, Alignment.END)
                     else
                       graph)
        .execute(test.algorithm)
        .writeTo(defaultSink)
      tracker.waitForJob()
      TestUtils.getResults(outputDirectory, tracker.getJobId)
    }

  private def normaliseResults(value: IterableOnce[String]) =
    value.iterator.toList.sorted.mkString("\n")

//  override def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder

  def setSpout(): Spout[String] = new IdentitySpout

  def assertResultsMatch(obtained: IterableOnce[String], resultsResource: String): Unit = {
    val source = scala.io.Source.fromResource(resultsResource)
    try assertResultsMatch(obtained, source.getLines())
    finally source.close()
  }

  def assertResultsMatch(obtained: IterableOnce[String], results: IterableOnce[String]): Unit =
    assertEquals(normaliseResults(obtained), normaliseResults(results))

  def correctnessTest(
      test: TestQuery,
      graphEdges: Edges,
      resultsResource: String
  ): IO[Unit] =
    Raphtory
      .newIOGraph()
      .use { g =>
        g.load(CSVEdgeListSource(setSpout(), 2,0,1))
        runTest(test, g)
      }
      .map(obtained => assertResultsMatch(obtained, resultsResource))

  def correctnessTest(
      test: TestQuery,
      graphEdges: Edges,
      results: Seq[String]
  ): IO[Unit] =
    Raphtory
      .newIOGraph()
      .use { g =>
        g.load(CSVEdgeListSource(setSpout(), 2,0,1))
        runTest(test, g)
      }
      .map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, results: Seq[String]): IO[Unit] =
    runTest(test).map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, graph: TemporalGraph, results: Seq[String]): IO[Unit] =
    runTest(test, graph).map(obtained => assertResultsMatch(obtained, results))

  def correctnessTest(test: TestQuery, results: String): IO[Unit] =
    runTest(test).map(obtained => assertResultsMatch(obtained, results))

}

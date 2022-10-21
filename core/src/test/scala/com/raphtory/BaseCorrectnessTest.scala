package com.raphtory

import cats.effect.IO
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.SequenceSpout

sealed trait View
case object Undirected extends View
case object Reverse    extends View

case class TestQuery(
    algorithm: GenericallyApplicable,
    timestamp: Long = -1,
    windows: List[Long] = List.empty[Long]
)

case class Edges(source: Source)

object Edges {
  implicit def edgesFromResource(resource: String): Edges = Edges(CSVEdgeListSource.fromResource(resource))
  implicit def edgesFromSource(source: Source): Edges     = Edges(source)
  implicit def sourceFromEdges(edges: Edges): Source      = edges.source

  implicit def edgesFromEdgeSeq(edges: Seq[String]): Edges = Edges(CSVEdgeListSource(SequenceSpout(edges: _*)))
}

trait Result

abstract class BaseCorrectnessTest(
    deleteResultAfterFinish: Boolean = true
) extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish) {

  private def runTest(test: TestQuery, graph: TemporalGraph): Iterator[String] = {
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
  ): Unit = {
    val obtained = {
      val graph = new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
        .newIOGraph(failOnNotFound = false, destroy = true)
        .allocated
        .unsafeRunSync()
      graph._1.load(graphEdges)
      runTest(test, graph._1)
    }
    assertResultsMatch(obtained, resultsResource)
  }

  def correctnessTest(
      test: TestQuery,
      graphEdges: Edges,
      resultsResource: String,
      graph: DeployedTemporalGraph
  ): Unit = {
    graph.load(graphEdges)
    val obtained = runTest(test, graph)
    assertResultsMatch(obtained, resultsResource)
  }

  def correctnessTest(
      test: TestQuery,
      graphEdges: Edges,
      results: Seq[String],
      graph: DeployedTemporalGraph
  ): Unit = {
    graph.load(graphEdges)
    val obtained = runTest(test, graph)
    assertResultsMatch(obtained, results)
  }

  def correctnessTest(
      test: TestQuery,
      results: Seq[String]
  ): Unit = {
    val obtained = {
      val graph = new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
        .newIOGraph(failOnNotFound = false, destroy = true)
        .allocated
        .unsafeRunSync()
      graph._1.load(setSource())
      runTest(test, graph._1)
    }
    assertResultsMatch(obtained, results)
  }

  def correctnessTest(
      test: TestQuery,
      view: View,
      results: Seq[String]
  ): Unit = {
    val obtained = {
      val graph = f()
      graph.load(setSource())
      view match {
        case Undirected => runTest(test, graph.undirectedView)
        case Reverse    => runTest(test, graph.reversedView)
      }
    }
    assertResultsMatch(obtained, results)
  }

  def correctnessTest(
      test: TestQuery,
      results: String
  ): Unit = {
    val obtained = {
      val graph = new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
        .newIOGraph(failOnNotFound = false, destroy = true)
        .allocated
        .unsafeRunSync()
      graph._1.load(setSource())
      runTest(test, graph._1)
    }
    assertResultsMatch(obtained, results)
  }

  def correctnessTest(
      test: TestQuery,
      results: String,
      graph: DeployedTemporalGraph
  ): Unit = {
    graph.load(setSource())
    val obtained = runTest(test, graph)
    assertResultsMatch(obtained, results)
  }

}

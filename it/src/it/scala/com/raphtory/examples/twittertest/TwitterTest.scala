package com.raphtory.examples.twittertest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.StaticGraphSpout
import munit.IgnoreSuite

import java.net.URL
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

/*
 * The Twitter dataset consists of 'circles' (or 'lists') from Twitter crawled from public sources.
 * The dataset includes node features (profiles), circles, and ego networks. Data is also available from Facebook
 * and Google+.
 * Dataset statistics
 * Nodes	81306
 * Edges	1768149
 * Nodes in largest WCC	81306 (1.000)
 * Edges in largest WCC	1768149 (1.000)
 * Nodes in largest SCC	68413 (0.841)
 * Edges in largest SCC	1685163 (0.953)
 * Average clustering coefficient	0.5653
 * Number of triangles	13082506
 * Fraction of closed triangles	0.06415
 * Diameter (longest shortest path)	7
 * 90-percentile effective diameter	4.5
 *
 * Reference: https://snap.stanford.edu/data/ego-Twitter.html
 *
 * */

@IgnoreSuite
class TwitterTest extends BaseRaphtoryAlgoTest[String] {
  override val outputDirectory: String = "/tmp/raphtoryTwitterTest"

  test("Connected Components Test") {
    algorithmPointTest(ConnectedComponents, 1400000)
      .map(assertEquals(_, "59ca85238e0c43ed8cdb4afe3a8a9248ea2c5497c945de6f4007ac4ed31946eb"))
  }

  override def setSource(): Source = CSVEdgeListSource(StaticGraphSpout("/tmp/twitter.csv"), delimiter = " ")

  def tmpFilePath = "/tmp/twitter.csv"

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(tmpFilePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"))
}

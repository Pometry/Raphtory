package com.raphtory.examples.bots

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.{AverageNeighbourDegree, Degree, Distinctiveness, PageRank}
import com.raphtory.algorithms.generic.community.{LPA, SLPA}
import com.raphtory.algorithms.generic.community.SLPA.{ChooseRandom, MostCommon, Rule}
import com.raphtory.algorithms.generic.motif.{LocalTriangleCount, ThreeNodeMotifs}
import com.raphtory.algorithms.generic.{AdjPlus, CBOD, ConnectedComponents, EdgeList, NeighbourNames, NodeList, TwoHopPaths}
import com.raphtory.algorithms.temporal.community.MultilayerLPA
import com.raphtory.algorithms.temporal.motif.MotifAlpha
import com.raphtory.algorithms.temporal.{Ancestors, Descendants, TemporalEdgeList, TemporalNodeList}
import com.raphtory.examples.bots.analysis.Influence
import com.raphtory.sinks.{FileSink, PulsarSink}
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "~/Raphtory/examples/raphtory-example-bots/src/main/python/BotDetection/raphtory.csv"

  val source  = FileSpout(path)
  val builder = new BotsGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("~/Raphtory/examples/raphtory-example-bots/src/main/python/RaphtoryOutput")

  val queryHandler = graph
    .execute(Influence())
    .writeTo(output)
    .waitForJob()


  graph.close()
}

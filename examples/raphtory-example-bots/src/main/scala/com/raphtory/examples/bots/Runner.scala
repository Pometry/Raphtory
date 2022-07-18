package com.raphtory.examples.bots

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.{Degree, PageRank}
import com.raphtory.algorithms.generic.community.{LPA, SLPA}
import com.raphtory.algorithms.generic.community.SLPA.{ChooseRandom, MostCommon, Rule}
import com.raphtory.algorithms.generic.motif.LocalTriangleCount
import com.raphtory.algorithms.generic.{CBOD, ConnectedComponents, EdgeList, NodeList, TwoHopPaths}
import com.raphtory.algorithms.temporal.community.MultilayerLPA
import com.raphtory.algorithms.temporal.motif.MotifAlpha
import com.raphtory.algorithms.temporal.{Ancestors, Descendants, TemporalEdgeList, TemporalNodeList}
import com.raphtory.sinks.{FileSink, PulsarSink}
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "/Users/pometry/Desktop/cleanedData5000/taa"

  val source  = FileSpout(path)
  val builder = new BotsFromJsonGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    .execute(Degree())
    .writeTo(output)
    .waitForJob()

  graph.close()
}

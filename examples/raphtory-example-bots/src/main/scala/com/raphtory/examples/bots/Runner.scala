package com.raphtory.examples.bots

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.algorithms.temporal.{TemporalEdgeList, TemporalNodeList}
import com.raphtory.sinks.{FileSink, PulsarSink}
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "/tmp/0aa"

  val source  = FileSpout(path)
  val builder = new BotsFromJsonGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    .execute(TemporalNodeList("Bot Label"))
//    .writeTo(PulsarSink("EdgeList_Bots"))
    .writeTo(output)

  queryHandler.waitForJob()

}

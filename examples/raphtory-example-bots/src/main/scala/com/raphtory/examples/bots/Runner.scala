package com.raphtory.examples.bots

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "/tmp/0ab"

  val source  = FileSpout(path)
  val builder = new BotsFromJsonGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    //    .at(1224377891)
    //    .at(1645937975) highest time test10
    //    .at(164460751)
//    .at(1627342929)
//    .past()
    .execute(EdgeList())
    .writeTo(output)

  //queryHandler.waitForJob()

}

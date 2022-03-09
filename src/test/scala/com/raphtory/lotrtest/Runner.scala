package com.raphtory.lotrtest

import com.google.protobuf.ByteString.Output
import com.raphtory.GlobalState
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.deploy.Raphtory
import com.raphtory.output.FileOutputFormat

object Runner extends App {

  val spout        = FileSpout("/tmp/lotr.csv")
  val graphBuilder = new LOTRGraphBuilder()
  val graph        = Raphtory.createGraph(spout, graphBuilder)
  graph.pointQuery(new GlobalState(), FileOutputFormat("/tmp"), 30000).waitForJob()
  graph.stop()

}

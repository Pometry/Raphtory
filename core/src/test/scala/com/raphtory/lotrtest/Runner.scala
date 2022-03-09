package com.raphtory.lotrtest

import com.raphtory.GlobalState
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout

object Runner extends App {

  val spout        = FileSpout("/tmp")
  val graphBuilder = new LOTRGraphBuilder()
  val graph        = Raphtory.batchLoadGraph(spout, graphBuilder)

}

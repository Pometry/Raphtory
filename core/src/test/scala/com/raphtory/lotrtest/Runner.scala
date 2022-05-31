package com.raphtory.lotrtest

import com.raphtory.GlobalState
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout

object Runner extends App {

  val spout        = FileSpout("/tmp")
  val graphBuilder = new LOTRGraphBuilder()
  val graph        = Raphtory.load(spout, graphBuilder)

  graph.deployment.stop()
  println("should finish here but these threads are still alive")

  Thread.getAllStackTraces.keySet().forEach { thread =>
    println(s"${thread.getName} with status ${thread.getState}")
  }
}

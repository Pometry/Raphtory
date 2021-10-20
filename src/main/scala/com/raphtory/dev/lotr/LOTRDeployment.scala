package com.raphtory.dev.lotr


import com.raphtory.algorithms.{ConnectedComponents, GraphState, TriangleCount}
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.spouts.FileSpout
import com.raphtory.core.components.querymanager.QueryManager.Message.Windows

object LOTRDeployment extends App{
  val source  = new FileSpout("datasets/lotr")
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)

  rg.pointQuery(GraphState("outputDir"),32000)
  rg.rangeQuery(ConnectedComponents("outputDir"),10000,32000,1000,Windows(10000, 1000,100))
  rg.pointQuery(TriangleCount("outputDir"),31816)


}







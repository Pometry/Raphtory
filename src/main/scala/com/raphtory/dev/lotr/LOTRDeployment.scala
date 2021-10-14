package com.raphtory.dev.lotr

import com.raphtory.algorithms.newer.{ConnectedComponents, GraphState}
import com.raphtory.core.build.server.RaphtoryPD

object LOTRDeployment extends App{
  val source  = new LOTRSpout()
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryPD[String](source,builder)
  rg.pointQuery(GraphState("/Users/bensteer/github/output"),32000)
  rg.rangeQuery(ConnectedComponents("/Users/bensteer/github/output"),10000,32000,1000,List(10000, 1000,100))

}

package com.raphtory.dev.lotr

import com.raphtory.algorithms.newer.{ConnectedComponents, GraphState, TriangleCount}

import com.raphtory.core.build.server.RaphtoryPD

object LOTRDeployment extends App{
  val source  = new LOTRSpout("src/main/scala/com/raphtory/dev/lotr/lotr.csv")
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryPD[String](source,builder)

  //rg.pointQuery(GraphState("/Users/bensteer/github/output"),32000)
  rg.pointQuery(TriangleCount("/Users/naomiarnold/CODE/lotroutput"),31816)
  //rg.rangeQuery(ConnectedComponents("/Users/bensteer/github/output"),10000,32000,1000,List(10000, 1000,100))

}

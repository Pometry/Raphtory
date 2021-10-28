package com.raphtory.dev.lotr


import com.raphtory.algorithms.TriangleCount
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.spouts.FileSpout

object LOTRDeployment extends App{
  val source  = new FileSpout("src/main/scala/com/raphtory/dev/lotr/")
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)

  rg.pointQuery(TriangleCount("/tmp/lotroutput"),31816)
}






//rg.pointQuery(GraphState("/Users/bensteer/github/output"),32000)
//rg.pointQuery(TriangleCount("/Users/naomiarnold/CODE/lotroutput"),31816)
//rg.rangeQuery(ConnectedComponents("/Users/bensteer/github/output"),10000,32000,1000,List(10000, 1000,100))

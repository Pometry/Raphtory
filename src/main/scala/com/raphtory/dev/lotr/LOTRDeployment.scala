package com.raphtory.dev.lotr

import com.raphtory.algorithms.newer.{BinaryDiffusion, LPA, ConnectedComponents, GraphState, TriangleCount}

import com.raphtory.core.build.server.{RaphtoryPD, RaphtoryGraph}
import net.openhft.hashing.LongHashFunction

object LOTRDeployment extends App{
  val source  = new LOTRSpout("src/main/scala/com/raphtory/dev/lotr/lotr.csv")
  val builder = new LOTRGraphBuilder()
  val rg = new RaphtoryGraph[String](source,builder)

  val startNode = LongHashFunction.xx3().hashChars("Gandalf")
  //rg.pointQuery(GraphState("/Users/bensteer/github/output"),32000)

  val gandalfid: Array[Long] = Array(startNode)
  val seedx = 10
  rg.pointQuery(new LPA(),31816)
  //rg.rangeQuery(ConnectedComponents("/Users/bensteer/github/output"),10000,32000,1000,List(10000, 1000,100))

}

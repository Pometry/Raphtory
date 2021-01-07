package com.raphtory

import algorithms.MotifCounting
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}

object testDeploy extends App {
  val source = new networkxSpout()
  val builder = new networkxGraphBuilder()
  val RG = RaphtoryGraph[String](source, builder)
  val arguments = Array[String]("4")
//  Thread.sleep(60000L)
  val motifs = new MotifCounting(arguments)

  RG.viewQuery(motifs, 4L, arguments)
}

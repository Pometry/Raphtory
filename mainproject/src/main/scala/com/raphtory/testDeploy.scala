package com.raphtory

import algorithms.{CBODMotifs, MotifCounting}
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}

object testDeploy extends App {
  val source = new networkxSpout()
  val builder = new networkxGraphBuilder()
  val RG = RaphtoryGraph[String](source, builder)

//  val arguments = Array[String]("3","3")
//  val motifs = new MotifCounting(arguments)
//  RG.viewQuery(motifs, 3L, arguments)
//
//
  val arguments = Array[String]("4","1")
  val motifs = new CBODMotifs(arguments) //delta, step
//  Thread.sleep(60000L)
  RG.viewQuery(motifs, 4L, arguments)
}

package com.raphtory

import com.raphtory.algorithms.MotifCounting
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}

object testDeploy extends App {
  val source = new networkxSpout()
  val builder = new networkxGraphBuilder()
  val RG = RaphtoryGraph[String](source, builder)

  val arguments = Array[String]("4","4")
  val motifs = new MotifCounting(arguments)
  Thread.sleep(30000L)
  RG.viewQuery(motifs, 4L, arguments)

//  val arguments = Array[String]("1510232000","1510232000")
//  val motifs = new MotifCounting(arguments) //delta, step
//  Thread.sleep(60000L)
//  RG.rangeQuery(DegreeBasic(), 1414772595000L, 1416282827000L,710232000L,1510232000L, arguments) //1510232
//  RG.rangeQuery(motifs,1414772595000L, 1416282827000L,710232000L,1510232000L, arguments)

}

package com.raphtory

import com.raphtory.algorithms.{CommunityOutlierDetection, LPA, MotifCounting}
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}

object testDeploy extends App {
  val source = new networkxSpout()
  val builder = new networkxGraphBuilder()
  val RG = RaphtoryGraph[String](source, builder)

  val arguments = Array[String]()//("4")
  val arguments2 = Array[String]("0","","500","0.1")
//  val motifs = new MotifCounting(arguments)
//  Thread.sleep(30000L)
//    RG.viewQuery(MotifCounting(arguments), 4L, arguments)
//  RG.rangeQuery(MotifCounting(arguments), 1L, 4L,1L,window = 2L , arguments)
  RG.viewQuery(LPA(arguments), 4L, arguments)
  RG.viewQuery(LPA(Array("1")), 4L, Array("1"))
  RG.viewQuery(CommunityOutlierDetection(arguments2), 4L, arguments2)
  RG.rangeQuery(CommunityOutlierDetection(arguments2), 1L, 4L, 1L,2L, arguments2)
//  Thread.sleep(10000L)
  RG.viewQuery(CommunityOutlierDetection(arguments), 4L, arguments)

//  val arguments = Array[String]("1510232000","1510232000")
//  val motifs = new MotifCounting(arguments) //delta, step
//  Thread.sleep(60000L)
//  RG.rangeQuery(DegreeBasic(), 1414772595000L, 1416282827000L,710232000L,1510232000L, arguments) //1510232
//  RG.rangeQuery(motifs,1414772595000L, 1416282827000L,710232000L,1510232000L, arguments)

}

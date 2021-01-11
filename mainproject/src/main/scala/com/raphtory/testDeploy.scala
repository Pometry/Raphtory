package com.raphtory

import algorithms.{CBODMotifs, DegreeBasic, LPA, MotifCounting}
import com.raphtory.spouts.FileSpout
import com.raphtory.testCases.blockchain.graphbuilders.bitcoin_mixers_GB
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}

object testDeploy extends App {
  val source = new FileSpout()
  val builder = new bitcoin_mixers_GB()
  val RG = RaphtoryGraph[String](source, builder)

//  val arguments = Array[String]("3","3")
//  val motifs = new MotifCounting(arguments)
//  RG.viewQuery(motifs, 3L, arguments)
//
//
  val arguments = Array[String]()//"1510232000","1510232000")
  val motifs = new LPA(arguments) //delta, step
//  Thread.sleep(60000L)
  RG.viewQuery(DegreeBasic(), 1416282827000L, arguments) //1510232
  RG.rangeQuery(motifs,1414772595000L, 1416282827000L,710232000L,1510232000L, arguments)
}

package com.raphtory.dev.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.util.Random

class datablast(args: Array[String]) extends Analyser[List[String]](args){
  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): List[String] ={
    println("Generating")
    val data= (for (i <- 1 to 1000000) yield
      Random.alphanumeric.take(10).mkString)
      .toList
    println("sending")
    data
  }




  override def defineMaxSteps(): Int = 1

  override def extractResults(results: List[List[String]]): Map[String, Any] = {
    println("If we make it this far the data has arrived")
    Map()
  }
}

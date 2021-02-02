package com.raphtory.test.oag

import com.raphtory.RaphtoryGraph
import com.raphtory.algorithms.{ConnectedComponents, DegreeBasic}
import com.raphtory.serialisers.JSONSerialiser
import com.raphtory.core.examples.oag.OAGGraphBuilder
//import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.MultiLineFileSpout

object OAGDeployment extends App{
//  val source  = new FileSpout()
  val source  = new MultiLineFileSpout()
  val builder = new OAGGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)
  val arguments = Array[String]()
  println(this.getClass)

  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,window=100,arguments)
//  rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,windowBatch=Array(10,50,100),arguments)

//  rg.viewQuery(DegreeBasic(),timestamp = 1,arguments)
//  rg.viewQuery(com.raphtory.serialisers.JSONSerialiser(),timestamp = 152672,arguments)
  // rg.viewQuery(DegreeBasic(),timestamp = 10000,window=100,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,windowBatch=Array(100,50,10),arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,windowBatch=Array(10,50,100),arguments)
}

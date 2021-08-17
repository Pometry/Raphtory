package com.raphtory.dev.lotr

import com.raphtory.RaphtoryGraph
import com.raphtory.algorithms.{ConnectedComponents, StateTest}
import com.raphtory.serialisers.MongoSerialiser

object LOTRDeployment extends App{
  val source  = new LOTRSpout()
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)
  val arguments = Array[String]()
//  rg.rangeQuery(new ConnectedComponents(Array()), new MongoSerialiser, start=1, end = 32674, increment=1000,windowBatch=List(10000, 1000,100), arguments)
  //rg.rangeQuery(SixDegreesOfGandalf(3),start = 1,end = 32674,increment = 100,arguments)

  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,window=100,arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,windowBatch=List(100,50,10),arguments)

  //rg.viewQuery(DegreeBasic(),timestamp = 10000,arguments)
  //rg.viewQuery(DegreeBasic(),timestamp = 10000,window=100,arguments)

}

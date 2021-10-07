package com.raphtory.dev.lotr

import com.raphtory.algorithms.{ConnectedComponents, StateTest}
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.serialisers.{DefaultSerialiser, MongoSerialiser}

object LOTRDeployment extends App{
  val source  = new LOTRSpout()
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryPD[String](source,builder)
  val arguments = Array("3")

  //rg.rangeQuery(TestAlgorithm(),10000,32000,1000,List(10000, 1000,100))
  //rg.rangeQuery(new ConnectedComponents(Array()), serialiser = new DefaultSerialiser, start=1, end = 32674, increment=1000,windowBatch=List(10000, 1000,100))
  //rg.viewQuery(SixDegreesOfGandalf(seperation = 3), serialiser = new DefaultSerialiser,timestamp = 5000)

  //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,arguments)
  //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,window=100,arguments)
  //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,windowBatch=List(100,50,10),arguments)

  //rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,arguments)
  //rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,window=100,arguments)

}

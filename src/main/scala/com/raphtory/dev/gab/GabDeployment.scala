package com.raphtory.dev.gab

import com.raphtory.algorithms.ConnectedComponents
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.spouts.FileSpout

object GabDeployment extends App{
  val source  = new FileSpout(directory = "src/main/scala/com/raphtory/dev/gab",fileName = "gabNetwork500.csv")
  val builder = new GabUserGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)
  val arguments = Array[String]()
  rg.rangeQuery(ConnectedComponents("/tmp"),start= 1470797917000L,end = 1476113868000L,increment = 86400000L,List(3600000L,86400000L,604800000L,2592000000L,31536000000L))

  //rg.oldrangeQuery(ConnectedComponents(),new DefaultSerialiser, start= 1470797917000L,end = 1476113868000L,increment = 86400000L,windowBatch=List(3600000L,86400000L,604800000L,2592000000L,31536000000L))
  //rg.oldrangeQuery(new TriangleCount(Array()), new DefaultSerialiser, start=1470797917000L,
  //  end = 1525368897000L,increment = 86400000L,window=86400000L)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,window=100,arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,windowBatch=Array(3600,36000,360000),arguments)

  //rg.viewQuery(DegreeBasic(),timestamp = 10000,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,window=100,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,windowBatch=Array(100,50,10),arguments)
}

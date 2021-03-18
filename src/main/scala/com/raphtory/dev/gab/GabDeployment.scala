package com.raphtory.dev.gab

import com.raphtory.RaphtoryGraph
import com.raphtory.algorithms.{ConnectedComponents, DegreeBasic}
import com.raphtory.examples.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.spouts.FileSpout

object GabDeployment extends App{
  val source  = new FileSpout()
  val builder = new GabUserGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)
  val arguments = Array[String]()
  Thread.sleep(60000)
  rg.rangeQuery(ConnectedComponents(),start = 1470797917000L,end = 1476113868000L,increment = 86400000L,windowBatch=Array(3600000L,86400000L,604800000L,2592000000L,31536000000L),arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,window=100,arguments)
  //rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,windowBatch=Array(3600,36000,360000),arguments)

  //rg.viewQuery(DegreeBasic(),timestamp = 10000,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,window=100,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,windowBatch=Array(100,50,10),arguments)
}
//curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","analyserName":"com.raphtory.algorithms.ConnectedComponents","start":1470797917000,"end":1525368897000,"jump":86400000,"windowType":"batched","windowSet":[31536000000,2592000000,604800000,86400000,3600000]}' 127.0.0.1:8081/RangeAnalysisRequest
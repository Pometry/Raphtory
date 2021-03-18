//package examples.lotr
//
//import com.raphtory.RaphtoryGraph
//import com.raphtory.algorithms.{ConnectedComponents, DegreeBasic}
//
//object LOTRDeployment extends App{
//  val source  = new LOTRSpout()
//  val builder = new LOTRGraphBuilder()
//  val rg = RaphtoryGraph[String](source,builder)
//  val arguments = Array[String]()
//
//  rg.rangeQuery(SixDegreesOfGandalf(3),start = 1,end = 32674,increment = 100,arguments)
//
//  rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,arguments)
//  rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,window=100,arguments)
//  rg.rangeQuery(ConnectedComponents(),start = 1,end = 32674,increment = 100,windowBatch=Array(100,50,10),arguments)
//
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,window=100,arguments)
//  rg.viewQuery(DegreeBasic(),timestamp = 10000,windowBatch=Array(100,50,10),arguments)
//}

package com.raphtory

import com.raphtory.algorithms.{CommunityOutlierDetection, ConnectedComponents, DegreeBasic, LPA, MotifCounting, MultiLayerLPA, MultiLayerLPAparams}
import com.raphtory.spouts.FileSpout
import com.raphtory.testCases.blockchain.graphbuilders.bitcoin_mixers_GB
import com.raphtory.testCases.networkx.{networkxGraphBuilder, networkxSpout}
import com.raphtory.testCases.wordSemantic.graphbuilders.{CoMatGBFiltered, CoMatParquetGB}
import com.raphtory.testCases.wordSemantic.spouts.{CoMatParquetSpout, CoMatSpout}
import org.apache.spark.sql.Row
object testDeploy extends App {
  val source = new CoMatParquetSpout()
  val builder = new CoMatParquetGB()
  val RG = RaphtoryGraph[Row](source, builder)

  //1414771239000, "end":1414871729000, "jump":3600000,"windowType":"true","windowSize":21600000, "args":["3600000", "BitCoin"]
  val arguments = Array[String]("21600000", "BitCoin", "10")
  val arguments2 = Array[String]("0","","50","2004","2009","1000000000","1")
//  val motifs = new MotifCounting(arguments)
//  Thread.sleep(30000L)
  RG.viewQuery(DegreeBasic(),2008000000010L, arguments2)
  RG.viewQuery(MultiLayerLPAparams(arguments2),2008000000010L, arguments2)
//  RG.viewQuery(ConnectedComponents(), 1414771239000L, arguments)
//  RG.viewQuery(LPA(arguments2), 1414771239000L, arguments2)
//    RG.viewQuery(MotifCounting(arguments), 1414771239000L, arguments)
//  RG.rangeQuery(MotifCounting(arguments), 1L, 4L,1L,window = 2L , arguments)
//  RG.viewQuery(MotifCounting(arguments), 1414871729000L, arguments)
//  RG.rangeQuery(MotifCounting(arguments), 1414771239000L, 1414871729000L, 21600000L, 86400000L,arguments)
//  RG.viewQuery(LPA(arguments), 3L, arguments)
//  RG.viewQuery(LPA(Array("1")), 4L, Array("0"))
//  RG.viewQuery(MultiLayerLPA(arguments2), 4L, arguments2)
//  RG.viewQuery(CommunityOutlierDetection(arguments2), 4L, arguments2)
//  RG.rangeQuery(CommunityOutlierDetection(arguments2), 1L, 4L, 1L,2L, arguments2)
//  Thread.sleep(10000L)
//  RG.viewQuery(CommunityOutlierDetection(arguments), 4L, arguments)

//  val arguments = Array[String]("1510232000","1510232000")
//  val motifs = new MotifCounting(arguments) //delta, step
//  Thread.sleep(60000L)
//  RG.rangeQuery(DegreeBasic(), 1414772595000L, 1416282827000L,710232000L,1510232000L, arguments) //1510232
//  RG.rangeQuery(motifs,1414772595000L, 1416282827000L,710232000L,1510232000L, arguments)

}

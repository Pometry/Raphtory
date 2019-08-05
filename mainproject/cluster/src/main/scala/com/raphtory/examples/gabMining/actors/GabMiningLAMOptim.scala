

package com.raphtory.examples.gabMining.actors

import java.time.LocalDateTime

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gabMining.analysis.{GabMiningAnalyser, GabMiningAnalyserOptim}
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabMiningLAMOptim (jobID:String)extends LiveAnalysisManager (jobID)  {
  val writing=new writeToFile()

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningAnalyserOptim()

  override protected def processResults(): Unit = {


    val resultsComing = results.asInstanceOf[ArrayBuffer[ParTrieMap[Int, Int]]]
    //println("Key values: "+ resultsComing)
    var finalResults=ParTrieMap[Int, Int]()

    for(kv <- resultsComing ){
      if(kv.nonEmpty){
        var  max= kv.maxBy(_._2)
        finalResults.put(max._1,max._2)
        // println("*********************El maximo eees: "+max)
        // println(max.getClass)
      }

    }
    if(finalResults.nonEmpty)
      println ("The star right now at" + LocalDateTime.now() +"is :",finalResults.maxBy(_._2))

  }

  override protected def processOtherMessages(value: Any): Unit = ""

}
//
//var maxInDegree=0
//var maxVertex=0
//// println("LAM RECEIVED RESULTS: "+ results)
//
//
//for ((vertex, degree) <- results){
//// println("********SUUBBBB LAAAMMM Vertex: "+ vertex +"Edges : "+ degree)
//if(degree.toString.toInt>maxInDegree) {
//maxInDegree= degree.toString.toInt
//maxVertex=vertex.toString.toInt
//
//}
//
//}
//println(" Vertex : " + maxVertex + " is a star with " + maxInDegree + " incoming edges")
//var text = maxVertex + "," + maxInDegree
//writing.writeLines("stars.csv", text)
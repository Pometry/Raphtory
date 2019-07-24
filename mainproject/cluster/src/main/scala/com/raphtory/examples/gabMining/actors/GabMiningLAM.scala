package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gabMining.analysis.GabMiningAnalyser
import com.raphtory.examples.gabMining.communications.VertexAndItsEdges


class GabMiningLAM(jobID:String) extends LiveAnalysisManager(jobID) {

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningAnalyser()

  override protected def processResults(): Unit = {
    var maxInDegree=0
    var maxVertex=0
   // println("LAM RECEIVED RESULTS: "+ results)

    for ((a, b) <- results.asInstanceOf[(Vector[(Int,Int)])]){
      // println("********SUUBBBB LAAAMMM Vertex: "+ a +"Edges : "+ b)
      if(b.toString.toInt>maxInDegree) {
        maxInDegree= b.toString.toInt
        maxVertex=a.toString.toInt
      }
//      else{
//        maxInDegree= b.toString.toInt
//        maxVertex=a.toString.toInt
//      }
    }
    println(" Vertex : " +maxVertex + " is a star with " +maxInDegree+" incoming edges")
  }

  override protected def processOtherMessages(value: Any): Unit = ""

}


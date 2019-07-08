package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gabMining.analysis.GabMiningAnalyser
import com.raphtory.examples.gabMining.communications.VertexAndItsEdges


class GabMiningLAM extends LiveAnalysisManager {

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningAnalyser()

  override protected def processResults(result: Any): Unit = {
    val results:Vector[Any] = result.asInstanceOf[(Vector[(Int,Int)])]
    var max=0
   // println("LAM RECEIVED RESULTS: "+ results)
    for ((a, b) <- results){

     // println("********SUUBBBB LAAAMMM Vertex: "+ a +"Edges : "+ b)
      if(b.toString.toInt>max) max= b.toString.toInt

    }
    print("****Max indegree: " + max)
    return max
  }

  override protected def processOtherMessages(value: Any): Unit = ""

}


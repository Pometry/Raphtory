package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gabMining.analysis.GabMiningAnalyser
import com.raphtory.examples.gabMining.communications.VertexAndItsEdges
import com.raphtory.examples.gabMining.utils.writeToFile


//<<<<<<< HEAD
class GabMiningLAM (jobID:String)extends LiveAnalysisManager (jobID)  {
  val writing=new writeToFile()
//=======
//class GabMiningLAM(jobID:String) extends LiveAnalysisManager(jobID) {
//>>>>>>> upstream/master

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningAnalyser()

  override protected def processResults(result: Any): Unit = {
    val results:Vector[Any] = result.asInstanceOf[(Vector[(Int,Int)])]
    var maxInDegree=0
    var maxVertex=0
   // println("LAM RECEIVED RESULTS: "+ results)

    for ((vertex, degree) <- results){
      // println("********SUUBBBB LAAAMMM Vertex: "+ vertex +"Edges : "+ degree)
      if(degree.toString.toInt>maxInDegree) {
        maxInDegree= degree.toString.toInt
        maxVertex=vertex.toString.toInt
      }

    }
    if (maxInDegree>100) {
      println(" Vertex : " + maxVertex + " is a star with " + maxInDegree + " incoming edges")
      var text = maxVertex + "," + maxInDegree
      writing.writeLines("stars.csv", text)

    }
  }

  override protected def processOtherMessages(value: Any): Unit = ""

}


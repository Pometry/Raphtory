package com.raphtory.examples.gabMining.actors

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningDensityAnalyser

import com.raphtory.examples.gabMining.utils.writeToFile

class GabMiningDensityLAM extends LiveAnalysisManager{
  val writing=new writeToFile()
  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDensityAnalyser()

  override protected def processResults(result: Any): Unit = {

    val results:Vector[Any] = result.asInstanceOf[(Vector[(Int,Long)])]
    var totalVertices=0
    var totalEdges=0

    println("*********INSIDE LAM: " + results)
    for ((vertices,edges) <- results){
      totalVertices+=vertices.toString.toInt
      totalEdges+=edges.toString.toInt
    }
    val density : Double= (totalEdges.toDouble/(totalVertices.toDouble*(totalVertices.toDouble-1)))
    println(f"Total vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    var text= totalVertices + ","+ totalEdges + ","+density

    writing.writeLines("Density2.csv",text)


  }

  override protected def processOtherMessages(value: Any): Unit = ""
}

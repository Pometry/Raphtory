package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, WorkerID}

import scala.collection.mutable.ArrayBuffer

class GabMiningDistribAnalyser extends Analyser{

  override def analyse()(implicit workerID:WorkerID): Any = {
    var results = ArrayBuffer[Int]()
    var maxInDegree=0
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val totalEdges= vertex.getIngoingNeighbors.size
      results+=totalEdges
    })
   results
  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }

}

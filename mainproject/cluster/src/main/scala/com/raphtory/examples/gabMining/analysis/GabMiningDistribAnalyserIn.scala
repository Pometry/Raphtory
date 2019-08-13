package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, WorkerID}

import scala.collection.mutable.ArrayBuffer

class GabMiningDistribAnalyserIn extends Analyser{

  override def analyse()(implicit workerID:WorkerID): Any = {
    var results = ArrayBuffer[Int]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val totalEdges= vertex.getIngoingNeighbors.size
      results+=totalEdges
    })
   // println("THIS IS HOW RESULTS LOOK: "+ results.groupBy(identity).mapValues(_.size))
   results.groupBy(identity).mapValues(_.size).toList
  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }

}


package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, WorkerID}

import scala.collection.mutable.ArrayBuffer

class GabMiningDistribAnalyserOut extends Analyser{

  override def analyse()(implicit workerID:WorkerID): Any = {
    var results = ArrayBuffer[Int]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val totalEdges= vertex.getOutgoingNeighbors.size
    //  println("Total edges for V "+v+" "+vertex.getOutgoingNeighbors + " "+vertex.getIngoingNeighbors )
      results+=totalEdges
    })
    // println("THIS IS HOW RESULTS LOOK: "+ results.groupBy(identity).mapValues(_.size))
    results.groupBy(identity).mapValues(_.size).toList
  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }

}


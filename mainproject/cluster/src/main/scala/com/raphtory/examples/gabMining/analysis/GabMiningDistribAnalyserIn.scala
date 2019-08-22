package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, WorkerID}

import scala.collection.mutable.ArrayBuffer

class GabMiningDistribAnalyserIn extends Analyser{

  //To obtain the in degree distribution of the network we need to know the number of time in where an specific degree took place. For example,
  // 10  nodes had an in-degree of 2 in a given time .
  // For this we need to loop over the vertices in the system and get their ingoing neighbours.
  //Then we store that to a results array that stores all the in-degrees. For the given example in the array we will find a value similar to :
  // (2,2,2,2,2,2,2,2,2,2) which is then grouped by its identifier, in this case the number 2. The output value is 2,10 and passed to the Live Analysis.

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


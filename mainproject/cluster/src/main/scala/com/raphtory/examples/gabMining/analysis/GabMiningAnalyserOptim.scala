
package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabMiningAnalyserOptim extends Analyser{



  override def analyse()(implicit workerID:WorkerID): Any = {

    var results = ParTrieMap[Int, Int]()

    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val inDegree= vertex.getIngoingNeighbors.size
      val currentInDegree= vertex.getOrSetCompValue("inDegree",v).asInstanceOf[Int]

      if(inDegree>currentInDegree){

        vertex.getOrSetCompValue("inDegree", inDegree)

        //results+=((v,inDegree))
        results.put(v, inDegree)

      }

    }
    results
  }

  override def setup()(implicit workerID: WorkerID) = {}

}


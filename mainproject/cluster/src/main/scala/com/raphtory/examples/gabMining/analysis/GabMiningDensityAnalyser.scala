package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepositoryProxies, VertexVisitor, WorkerID}
import akka.actor.ActorContext
import monix.execution.atomic.AtomicLong

import scala.collection.parallel.mutable.ParTrieMap


class GabMiningDensityAnalyser extends Analyser {
  override def analyse()(implicit workerID:WorkerID): Any = {
    var totalDegree: Int = 0
    var totalNodes: Int = 0

   // println("Worker: " + workerID +" Here functions nodes:"+proxy.getVerticesSet().size+" Degree:  "+proxy.getEdgesSet().size)
   // (proxy.getVerticesSet().size,proxy.getEdgesSet().size)
    //var results = ParTrieMap[Int, Int]()

    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val degree= vertex.getAllNeighbors.size

      totalDegree += degree
      totalNodes+=1

    }
   // println("Worker: " + workerID +" Here NO functions nodes :"+totalNodes+" Degree:  "+totalDegree)
    (totalNodes,totalDegree)

  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }
}

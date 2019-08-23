package com.raphtory.core.analysis.Algorithms.Density

import com.raphtory.core.analysis.{Analyser, WorkerID}

// to obtain the density of the network we need to obtain the degree and the number of vertices to plug in the
// formula that is executed by the Live Analysis.
//For this , we need to loop over the vertices that are currently in memory in the system, then for each vertex ,
//we need to get the number of its neighbours so we can obtain the number of edges to be sent.
//the values are sent in a simple array tuple formed by two integers

class DensityAnalyser extends Analyser {
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

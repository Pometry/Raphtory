package com.raphtory.api

import com.raphtory.core.analysis.entity.Vertex
import org.codehaus.jackson.map.ser.std.StdJdkSerializers.FileSerializer


abstract class GraphAlgorithm[T<:Any] {
    def workflow(perspective:GraphPerspective[T])
}

class GraphPerspective[T<:Any] {

  def map(f:(Vertex)=>T):GraphResults[T] ={
    null
  }
  def singleStep(f:(Vertex)=>Unit):GraphPerspective[T]= {this}
  def multiStep(f:(Vertex)=>Unit,steps:Int):GraphPerspective[T] = {this}
}

 class GraphResults[T<:Any] {
  //def merge(results:List[T]):
}

class OutDegree extends GraphAlgorithm[Int]{
  override def workflow(perspective: GraphPerspective[Int]): Unit = {
    perspective.map(vertex=> vertex.getOutEdges.size)
  }
}

class ConnectedComponents extends GraphAlgorithm[Int] {
  override def workflow(perspective: GraphPerspective[Int]): Unit = {
    perspective.singleStep({
      vertex =>
        vertex.setState("cclabel", vertex.ID)
        vertex.messageAllNeighbours(vertex.ID)
    })
      .multiStep({
        vertex =>
        val label  = vertex.messageQueue[Long].min
        if (label < vertex.getState[Long]("cclabel")) {
          vertex.setState("cclabel", label)
          vertex messageAllNeighbours label
        }
        else
          vertex.voteToHalt()
      },10).map(vertex => vertex.getState[Long]("cclabel").toInt)
  }
}
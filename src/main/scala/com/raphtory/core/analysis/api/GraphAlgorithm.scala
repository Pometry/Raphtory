package com.raphtory.core.analysis.api

import com.raphtory.core.analysis.entity.Vertex
import org.codehaus.jackson.map.ser.std.StdJdkSerializers.FileSerializer


abstract class GraphAlgorithm {
  def workflow(graph:GraphPerspective)
}

class GraphPerspective{
  def select[S](f:Vertex=>S):GraphResult[S] ={new GraphResult(List[S]())}
  def singleStep(f:(Vertex)=>Unit):GraphPerspective= {this}
  def multiStep(f:(Vertex)=>Unit,steps:Int):GraphPerspective = {this}
}

class GraphResult[S](results:List[S]){
  def combine[T](f:(List[S])=>T) = new CombinedGraphResult[T](new T())
  def serialise(f:(List[S])=>Map[String,Any]): Unit = {}
}

class CombinedGraphResult[S](results:S){
  def reduce[T](f:(List[S])=>T) = new ReducedGraphResult[T](new T())
  def serialise(f:(List[S])=>Map[String,Any]): Unit = {}
}

class ReducedGraphResult[S](results:S){
  def serialise(f:(S)=>Map[String,Any]): Unit = {}
}


class OutDegreeAverage extends GraphAlgorithm{
  override def workflow(graph: GraphPerspective): Unit = {
    graph
      .select(vertex=>vertex.getOutEdges.size)
      .combine(outGoingCounts => (outGoingCounts.sum,outGoingCounts.size))
      .reduce(partitionResult => {
        val totalVertices      = partitionResult.map(x=>x._2).sum.toDouble
        val totalOutgoingEdges = partitionResult.map(x=>x._1).sum.toDouble
        (totalVertices,totalOutgoingEdges)
      })
      .serialise(result=> {
        Map("Vertices" ->result._1,
            "Edges"      ->result._2,
            "Out Degree" ->result._1/result._2
        )
      })
  }
}

class ConnectedComponents extends GraphAlgorithm {
  override def workflow(graph: GraphPerspective): Unit = {
    graph
      .singleStep({
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
      },100)
      .select(vertex => vertex.getState[Long]("cclabel"))
      .combine(IDs =>
        IDs.groupBy(f=>f)
           .map(f => (f._1, f._2.size))
      )
      .reduce(partialGroups =>
        partialGroups
          .flatten
          .groupBy(f => f._1)
          .mapValues(x => x.map(_._2).sum)
      )
      .serialise(groups => {
        val groupedNonIslands = groups.filter(x => x._2 > 1)
        val sorted = groupedNonIslands.toArray.sortBy(_._2)(sortOrdering).map(x=>x._2)
        val top5 = if(sorted.length<=5) sorted else sorted.take(5)
        val totalWithoutIslands = groupedNonIslands.size
        val totalIslands = groups.size - totalWithoutIslands
        val proportion = groups.maxBy(_._2)._2.toFloat / groups.values.sum
        Map("top5"         -> top5,
            "total"        -> groups.size,
            "totalIslands" -> totalIslands,
            "proportion"   -> proportion)
      })
  }
}

object sortOrdering extends Ordering[Int] {
  def compare(key1: Int, key2: Int) = key2.compareTo(key1)
}

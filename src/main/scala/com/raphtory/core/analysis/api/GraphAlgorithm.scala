package com.raphtory.core.analysis.api

import com.raphtory.core.analysis.entity.Vertex
import org.codehaus.jackson.map.ser.std.StdJdkSerializers.FileSerializer


abstract class GraphAlgorithm {
  def algorithm(graph:GraphPerspective)
}

class GraphPerspective{
  def filter(f:(Vertex)=>Boolean):GraphPerspective= {this}
  def step(f:(Vertex)=>Unit):GraphPerspective= {this}
  def iterate(f:(Vertex)=>Unit, iterations:Int):GraphPerspective = {this}
  def select[S](f:Vertex=>S):VertexResult[S] ={new VertexResult(List[S]())}
}

class VertexResult[S](results:List[S]){
  def combine[T](f:(List[S])=>T) = new PartitionResult[T](new T())
  def collect():CollectedResult[S] = new CollectedResult[S](List[S]())
  def filter[T] = {}
  def distributedSave(f:(S)=>Map[String,Any]): Unit = {}
}

class PartitionResult[S](results:S){
  def reduce[T](f:(List[S])=>T) = new GlobalResult[T](new T())
}

class GlobalResult[S](results:S){
  def save(f:(S)=>Map[String,Any]): Unit = {}
}

class CollectedResult[S](results:List[S]){
  def save(f:(List[S])=>Map[String,Any]): Unit = {}
}

class OutDegreeAverage extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select(vertex=>vertex.getOutEdges.size)
      .combine(outGoingCounts => (outGoingCounts.sum,outGoingCounts.size))
      .reduce(partitionResult => {
        val totalVertices      = partitionResult.map(x=>x._2).sum.toDouble
        val totalOutgoingEdges = partitionResult.map(x=>x._1).sum.toDouble
        (totalVertices,totalOutgoingEdges)
      })
      .save(result=> {
        Map("Vertices"   ->result._1,
            "Edges"      ->result._2,
            "Out Degree" ->result._1/result._2
        )
      })
  }
}

class ConnectedComponentsCombineReduce extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
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
      .save(groups => {
        val total =groups.size
        Map("total" -> total)
      })
  }
}


class ConnectedComponentsCollect extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[Long].min
          if (label < vertex.getState[Long]("cclabel")) {
            vertex.setState("cclabel", label)
            vertex messageAllNeighbours label
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => vertex.getState[Long]("cclabel"))
      .collect()
      .save(IDs => {
        val total = IDs.groupBy(f => f)
          .map(f => (f._1, f._2.size))
          .size
        Map("total" -> total)
      })
  }
}

class ConnectedComponentsDistributedSave extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[Long].min
          if (label < vertex.getState[Long]("cclabel")) {
            vertex.setState("cclabel", label)
            vertex messageAllNeighbours label
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => (vertex.ID(),vertex.getState[Long]("cclabel")))
      .distributedSave(vertexResult => {
        Map("ID"      -> vertexResult._1,
            "ccLabel" -> vertexResult._2)

      })

  }
}


object sortOrdering extends Ordering[Int] {
  def compare(key1: Int, key2: Int) = key2.compareTo(key1)
}

//      .save(groups => {
//        val groupedNonIslands = groups.filter(x => x._2 > 1)
//        val sorted = groupedNonIslands.toArray.sortBy(_._2)(sortOrdering).map(x=>x._2)
//        val top5 = if(sorted.length<=5) sorted else sorted.take(5)
//        val totalWithoutIslands = groupedNonIslands.size
//        val totalIslands = groups.size - totalWithoutIslands
//        val proportion = groups.maxBy(_._2)._2.toFloat / groups.values.sum
//        Map("top5"         -> top5,
//            "total"        -> groups.size,
//            "totalIslands" -> totalIslands,
//            "proportion"   -> proportion)
//      })
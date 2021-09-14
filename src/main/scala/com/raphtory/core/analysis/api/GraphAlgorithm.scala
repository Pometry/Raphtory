package com.raphtory.core.analysis.api

import com.raphtory.core.analysis.entity.Vertex



abstract class GraphAlgorithm {
  def algorithm(graph:GraphPerspective)
}
//at all of these steps do we have access to time time + window
class GraphPerspective{ //are these mutable
  def filter(f:(Vertex)=>Boolean):GraphPerspective= {this} //delete from graph completely
  def step(f:(Vertex)=>Unit):GraphPerspective= {this}
  def iterate(f:(Vertex)=>Unit, iterations:Int):GraphPerspective = {this}
  def select[S](f:Vertex=>S):VertexResult[S] ={new VertexResult(List[S]())}
}

class VertexResult[S](results:List[S]){
  def collect():CollectedResult[S] = new CollectedResult[S](List[S]())
  def filter(f:(S)=> Boolean):VertexResult[S] = {this}
  def distributedSave(f:(S)=>Map[String,Any]): Unit = {}
}

class CollectedResult[S](results:List[S]){ // convert to just extract the results
  def save(f:(List[S])=>Map[String,Any]): Unit = {}
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



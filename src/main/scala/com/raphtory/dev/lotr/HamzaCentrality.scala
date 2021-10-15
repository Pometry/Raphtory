package com.raphtory.dev.lotr

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class HamzaCentrality extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({ vertex =>
        val vertexSet = Seq(vertex.ID())

        vertex.setState("vertexSet", vertexSet)
        vertex.messageAllNeighbours(vertexSet)
      })
      .iterate({
        vertex =>
          if(!vertex.hasMessage()) {
            vertex.voteToHalt()
          } else {
            val neighbourSets = vertex.messageQueue[Seq[Long]]
            val mySet = vertex.getState[Seq[Long]]("vertexSet")

            val differenceSets = neighbourSets.map { set => set.diff(mySet) }
            val mergedDifferences = differenceSets.flatten.distinct

            val newSet = mySet.union(mergedDifferences)

            vertex.setState("vertexSet", newSet)

            vertex.messageAllNeighbours(newSet)
          }
      }, 100)
      .select { vertex =>
        val degree = vertex.getState[Seq[Long]]("vertexSet").size
        val characterName = vertex.getPropertyValue("name").fold("unknown")(_.asInstanceOf[String])

        Row(Array(characterName, degree))
      }
      .writeTo("/Users/alnaimi/dev/output")
  }
}
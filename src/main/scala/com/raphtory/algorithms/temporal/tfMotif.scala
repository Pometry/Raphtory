package com.raphtory.algorithms.temporal

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

/**
Description
  The algorithms identifies 2-edge-1-node temporal motifs.
  For each incoming edge a vertex has, the algorithm checks whether there are any outgoing 
  edges which occur after it and returns a count of these.
  
Parameters
  output_dir (String) : Directory path where the output is written to. (Default: "/tmp/tfmotif")

Returns
  ID (Long) : Vertex ID
  Number of motifs (Long) : Number of motifs.
**/
class tfMotif(fileOutput:String="/tmp/tfmotif") extends GraphAlgorithm {

  override def tabularise(graph: GraphPerspective): Table = {
    graph
      .select({
        vertex =>
          if (vertex.explodeInEdges().nonEmpty & vertex.explodeOutEdges().nonEmpty) {
            val out = vertex.explodeInEdges()
              .map(inEdge => vertex.explodeOutEdges()
                .filter(e => e.getTimestamp() > inEdge.getTimestamp() & e.dst() != inEdge.src() & e.dst() != inEdge.dst()).size).sum
            Row(vertex.name(), out)
          }
          else {
            Row(vertex.name(), 0)
          }
      }
      )
  }

  override def write(table: Table): Unit = {
    table.writeTo(fileOutput)
  }

}

object tfMotif{
  def apply(path:String) = new tfMotif(path)
}


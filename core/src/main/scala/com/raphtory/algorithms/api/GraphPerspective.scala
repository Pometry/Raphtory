package com.raphtory.algorithms.api

import com.raphtory.graph.visitor
import com.raphtory.graph.visitor.ExplodedVertex
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.Vertex
import scala.collection.immutable.Queue


/** Core Public interface of the algorithm API
  * This implements the operations exposed by GraphOperations returning
  * a new GraphPerspective for those operations that have a graph as a result.
  *
  * @see [[com.raphtory.algorithms.api.GraphOperations]]
  */

trait GraphPerspective extends GraphOperations {
  override type G <: GraphPerspective
  override type MG <: MultilayerGraphPerspective
  override type RG <: GraphPerspective
}

trait MultilayerGraphPerspective extends GraphPerspective with GraphOperations {
  override type G <: MultilayerGraphPerspective
  override type V <: ExplodedVertex
}

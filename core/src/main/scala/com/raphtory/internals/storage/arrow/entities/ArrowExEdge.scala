package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.{ConcreteExplodedEdge, ReducedEdge}
import com.raphtory.arrowcore.model.{Edge, Entity}
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository

class ArrowExEdge(edge: Edge, protected val repo: ArrowEntityStateRepository) extends ReducedEdge with ArrowExEntity {

  /** type of vertex IDs for this edge */
  override type IDType = Long

  /** Edge ID */
  override def ID: Long = entity.getGlobalId

  /** ID of the source vertex of the edge */
  override def src: Long =
    if (!edge.isSrcGlobal)
      repo.asGlobal(edge.getSrcVertex)
    else edge.getSrcVertex

  /** ID of the destination vertex of the edge */
  override def dst: Long =
    if (!edge.isDstGlobal)
      repo.asGlobal(edge.getDstVertex)
    else edge.getDstVertex

  /** Filter the edge from the `GraphPerspective`. */
  override def remove(): Unit =
    repo.removeEdge(edge.getGlobalId)

  /** Send a message to the vertex connected on the other side of the edge
    *
    * @param data Message data to send
    */
  override def send(data: Any): Unit = {
    val vertexId = if (isIncoming) src else dst
    repo.sendMessage(VertexMessage(repo.superStep, vertexId, data))
  }

  override def entity: Entity = edge

  /** Remove an entry in the entity's algorithmic state. */
  override def clearState(key: String): Unit = ???

  /** concrete type for exploded edge views of this edge which implements
    * [[ExplodedEdge]] with same `IDType`
    */
  override type ExplodedEdge = ConcreteExplodedEdge[Long]

  /** Return an [[ExplodedEdge]] instance for each time the edge is
    * active in the current view.
    */
  override def explode(): List[ExplodedEdge] = List.empty
}

package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.{ConcreteEdge, Edge => REdge}
import com.raphtory.arrowcore.model.{Edge, Entity}
import com.raphtory.internals.storage.arrow.{ArrowGraphLens, ArrowPartition, ArrowEntityStateRepository}

class ArrowExEdge(edge:Edge, protected val repo: ArrowEntityStateRepository) extends ConcreteEdge[Long] with ArrowExEntity {
  /** type of vertex IDs for this edge */
  override type IDType = Long

  /** Edge ID */
  override def ID: Long = entity.getGlobalId

  /** ID of the source vertex of the edge */
  override def src: Long = edge.getSrcVertex // TODO: global or local?

  /** ID of the destination vertex of the edge */
  override def dst: Long = edge.getDstVertex //TODO: global or local?

  /** Filter the edge from the `GraphPerspective`. */
  override def remove(): Unit = {
    repo.removeEdge(edge.getLocalId)
  }

  /** Send a message to the vertex connected on the other side of the edge
   *
   * @param data Message data to send
   */
  override def send(data: Any): Unit = ???

  override def entity: Entity = edge
}

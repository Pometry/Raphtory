package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.{Vertex => ArrVertex}
import com.raphtory.internals.components.querymanager.FilteredEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage

import scala.collection.View
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository
import com.raphtory.internals.storage.arrow.RichVertex

class ArrowExVertex(val repo: ArrowEntityStateRepository, val vertex: ArrVertex) extends Vertex with ArrowExEntity {

  def entity: Entity = vertex

  /** ID type of this vertex */
  override type IDType = Long

  /** Concrete edge type for this vertex which implements [[com.raphtory.api.analysis.visitor.Edge Edge]] */
  override type Edge = ArrowExEdge

  /** implicit ordering object for use when comparing vertex IDs */
  implicit override val IDOrdering: Ordering[Long] = Ordering.Long

  /** Get the ID type of this vertex */
  override def ID: Long = entity.getGlobalId

  /** Check if vertex has received messages */
  override def hasMessage: Boolean = repo.hasMessage(ID)

  /** Queue of received messages
    *
    * @tparam `T` message data type
    */
  override def messageQueue[T]: View[T] =
    repo.releaseQueue(ID)

  /** Vote to stop iterating (iteration stops if all vertices voted to halt) */
  override def voteToHalt(): Unit = repo.vertexVoted()

  /** Send data to another vertex at next Step/Iteration
    *
    * @param vertexId Vertex Id of target vertex for the message
    * @param data     message data to send
    */
  override def messageVertex(vertexId: IDType, data: Any): Unit =
    repo.sendMessage(VertexMessage(repo.superStep + 1, vertexId, data))

  /** Return all edges starting at this vertex
    */
  override def outEdges: View[ArrowExEdge] =
    vertex.outgoingEdges
      .map(new ArrowExEdge(_, repo))
      .filter(e => repo.isEdgeAlive(e.src, e.dst))

  /** Return all edges ending at this vertex
    */
  override def inEdges: View[ArrowExEdge] =
    vertex.incomingEdges
      .map(new ArrowExEdge(_, repo))
      .filter(e => repo.isEdgeAlive(e.src, e.dst))

  /** Return specified edge if it is an out-edge of this vertex
    *
    * @param id ID of edge to return
    */
  override def getOutEdge(id: Long): Option[Edge] = ???

  /** Return specified edge if it is an in-edge of this vertex
    *
    * @param id ID of edge to return
    */
  override def getInEdge(id: Long): Option[Edge] = ???

  /** Return specified edge if it is an in-edge or an out-edge of this vertex
    *
    * This function returns a list of edges, where the list is empty if neither an in-edge nor an out-edge
    * with this id exists, contains one element if either an in-edge or an out-edge with the id exists, or
    * contains two elements if both in-edge and out-edge exist.
    *
    * @param id ID of edge to return
    */
  override def getEdge(id: Long): View[Edge] = ???

  /** Filter this vertex and remove it and all its edges from the GraphPerspective */
  override def remove(): Unit = {
    repo.removeVertex(ID)
    // all outgoing edges are present here on this node where the vertex is removed from
    outEdges.foreach { edge =>
      val other = if (edge.src == ID) edge.dst else edge.src
      repo.sendMessage(FilteredOutEdgeMessage(repo.superStep, other, ID))
    }
    inEdges.foreach { edge =>
      val other = if (edge.src == ID) edge.dst else edge.src
      repo.sendMessage(FilteredInEdgeMessage(repo.superStep, other, ID))
    }
  }

  def explode(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  ): Vertex = {
    vertex
    ???
  }
}

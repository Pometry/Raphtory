package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.{Vertex => ArrVertex}
import com.raphtory.internals.storage.GraphExecutionState

import scala.collection.View
import com.raphtory.internals.storage.arrow.{ArrowEntityStateRepository, RichVertex}

class ArrowExVertex(val repo: ArrowEntityStateRepository, vertex: ArrVertex) extends Vertex with ArrowExEntity {

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
  override def hasMessage: Boolean = repo.hasMessage(entity.getLocalId)

  /** Queue of received messages
    *
    * @tparam `T` message data type
    */
  override def messageQueue[T]: List[T] = ???

  /** Vote to stop iterating (iteration stops if all vertices voted to halt) */
  override def voteToHalt(): Unit = ???

  /** Send data to another vertex at next Step/Iteration
    *
    * @param vertexId Vertex Id of target vertex for the message
    * @param data     message data to send
    */
  override def messageVertex(vertexId: Long, data: Any): Unit = ???

  /** Return all edges starting at this vertex
    */
  override def outEdges: View[ArrowExEdge] = vertex.outgoingEdges.map(new ArrowExEdge(_, repo))

  /** Return all edges ending at this vertex
    */
  override def inEdges: View[ArrowExEdge] = vertex.incomingEdges.map(new ArrowExEdge(_, repo))

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
  override def remove(): Unit = ???

}

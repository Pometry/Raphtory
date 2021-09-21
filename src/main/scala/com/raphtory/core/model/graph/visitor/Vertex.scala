package com.raphtory.core.model.graph.visitor

import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.entities.internal.RaphtoryVertex
import com.raphtory.core.implementations.objectgraph.messaging.VertexMultiQueue

import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap
import scala.reflect.ClassTag

trait Vertex extends EntityVisitor {

  def ID():Long

  //functionality for checking messages
  def hasMessage(): Boolean
  def messageQueue[T: ClassTag]: List[T]
  def voteToHalt(): Unit
  //Send message
  def messageNeighbour(vertexId: Long, data: Any): Unit
  def messageAllOutgoingNeighbors(message: Any): Unit
  def messageAllNeighbours(message: Any)
  def messageAllIngoingNeighbors(message: Any): Unit

  //all edges
  def getEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge]
  //all out edges
  def getOutEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge]
  //all in edges
  def getInEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge]
  //individual out edge
  def getOutEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[Edge]
  //individual in edge
  def getInEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[Edge]


  // analytical state
  def setState(key: String, value: Any): Unit
  def getState[T: ClassTag](key: String):T
  def containsState(key: String): Boolean
  def getOrSetState[T: ClassTag](key: String, value: T): T
  def appendToState[T: ClassTag](key: String, value: Any):Unit

  // Also need a function for receiving messages, but the user should not have access to this
  //private def receiveMessage(msg: VertexMessage): Unit
}

package com.raphtory.core.model.graph.visitor



import scala.reflect.ClassTag

trait Vertex extends EntityVisitor {

  def ID():Long

  //functionality for checking messages
  def hasMessage(): Boolean
  def messageQueue[T: ClassTag]: List[T]
  def voteToHalt(): Unit
  //Send message
  def messageSelf(data: Any):Unit
  def messageNeighbour(vertexId: Long, data: Any): Unit
  def messageAllOutgoingNeighbors(message: Any): Unit
  def messageAllNeighbours(message: Any)
  def messageAllIngoingNeighbors(message: Any): Unit

  //Get Neighbours
  def getAllNeighbours(after:Long=0L,before:Long=Long.MaxValue): List[Long]
  def getOutNeighbours(after:Long=0L,before:Long=Long.MaxValue): List[Long]
  def getInNeighbours(after:Long=0L,before:Long=Long.MaxValue): List[Long]

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

  //all edges
  def explodeEdges(after:Long=0L,before:Long=Long.MaxValue): List[ExplodedEdge]
  //all out edges
  def explodeOutEdges(after:Long=0L,before:Long=Long.MaxValue): List[ExplodedEdge]
  //all in edges
  def explodeInEdges(after:Long=0L,before:Long=Long.MaxValue): List[ExplodedEdge]
  //individual out edge
  def explodeOutEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[List[ExplodedEdge]]
  //individual in edge
  def explodeInEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[List[ExplodedEdge]]


  // analytical state
  def setState(key: String, value: Any): Unit
  def getState[T: ClassTag](key: String):T
  def getStateOrElse[T: ClassTag](key: String,value:T):T
  def containsState(key: String): Boolean
  def getOrSetState[T: ClassTag](key: String, value: T): T
  def appendToState[T: ClassTag](key: String, value: Any):Unit

  // Also need a function for receiving messages, but the user should not have access to this
  //private def receiveMessage(msg: VertexMessage): Unit
}

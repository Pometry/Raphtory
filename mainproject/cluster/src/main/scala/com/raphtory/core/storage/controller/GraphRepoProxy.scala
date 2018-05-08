package com.raphtory.core.storage.controller

import akka.actor.ActorContext
import com.raphtory.core.analysis.VertexVisitor
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.{MemoryConnector, ReaderConnector, RedisConnector}
import com.raphtory.core.utils.KeyEnum
object GraphRepoProxy {

  private val connectors : Array[ReaderConnector] = Array(MemoryConnector, RedisConnector)

  private var edgesSet : Set[Long] = Set[Long]()
  private var verticesSet : Set[Long] = Set[Long]()

  def apply(): Unit = {
  }

  //def iterativeApply(f : Connector => Unit)

  def entityExists(entityType : KeyEnum.Value, id : Long) : Boolean = {
    connectors.foreach(conn => {
      if (conn.lookupEntity(entityType, id))
        return true
    })
    false
  }

  def addEdge(id : Long) = {
    edgesSet += id
  }

  def addVertex(id : Long) = {
    verticesSet += id
  }

  def getEdgesSet() : Set[Long] = {
    edgesSet
  }

  def getVerticesSet() : Set[Long] = {
    verticesSet
  }

  def getVertex(id : Long)(implicit context : ActorContext, managerCount : Int) : VertexVisitor = {
    connectors.foreach(c => {
      c.getEntity(KeyEnum.vertices, id) match {
        case Some(v) => return VertexVisitor(v.asInstanceOf[Vertex])
        case None    =>
      }
    })
    return null
  }

  def getEdge(id : Long) : Edge = {
    connectors.foreach(c => {
      c.getEntity(KeyEnum.edges, id) match {
        case Some(edge) => return  edge.asInstanceOf[Edge]
        case None    =>
      }
    })
    return null
  }
}

package com.raphtory.core.storage.controller

import akka.actor.ActorContext
import com.raphtory.core.analysis.VertexVisitor
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.{MemoryConnector, ReaderConnector}//, RedisConnector}
import com.raphtory.core.utils.KeyEnum

import scala.collection.parallel.ParSet
object GraphRepoProxy {

  //private val connectors : Array[ReaderConnector] = Array(MemoryConnector)//, RedisConnector)

  private var edgesSet : ParSet[Long] = ParSet[Long]()
  private var verticesSet : ParSet[Long] = ParSet[Long]()

  def apply(): Unit = {
  }

  //def iterativeApply(f : Connector => Unit)



  def addEdge(id : Long) = {
    edgesSet += id
  }

  def addVertex(id : Long) = {
    verticesSet += id
  }

  def getEdgesSet() : ParSet[Long] = {
    edgesSet
  }

  def getVerticesSet() : ParSet[Long] = {
    verticesSet
  }

  def getVertex(id : Long)(implicit context : ActorContext, managerCount : Int) : VertexVisitor = {

    return null
  }

  def getEdge(id : Long) : Edge = {

    return null
  }
}

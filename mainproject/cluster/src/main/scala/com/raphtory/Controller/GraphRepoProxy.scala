package com.raphtory.Controller

import com.raphtory.Storage.{MemoryConnector, ReaderConnector, RedisConnector}
import com.raphtory.utils.KeyEnum
object GraphRepoProxy {
  private val connectors : Array[ReaderConnector] = Array(MemoryConnector, RedisConnector)

  private var edgesSet : Set[Long] = Set[Long]()
  private var verticesSet : Set[Long] = Set[Long]()

  def apply(): Unit = {
  }

  // TODO iterative apply
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

  // TODO
  // getHistory
  // getProperties
  // getProperty
  // getEntity

}

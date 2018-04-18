package com.raphtory.Storage
import com.raphtory.utils.KeyEnum

trait WriterConnector {
  def addEntity(entityType : KeyEnum.Value, entityId : Long, creationTime : Long)
  def addState(entityType : KeyEnum.Value, entityId : Long, timestamp : Long, value : Boolean)
  def addProperty(entityType : KeyEnum.Value, entityId : Long, key : String, timestamp : Long, value : String)

  def addAssociatedEdge(vertexId : Long, edgeId : Long)
}

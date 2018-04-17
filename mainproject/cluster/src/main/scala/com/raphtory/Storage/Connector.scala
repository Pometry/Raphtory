package com.raphtory.Storage
import com.raphtory.GraphEntities.Entity
import com.raphtory.utils.KeyEnum

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
trait Connector {
  def rangeQuery(entityType : String, entityId : Long, property : String, startTime : Long, endTime : Long) : Any

  def lookupEntity(entityType : String, entityId : Long) : Boolean
  def getEntity(entityType : KeyEnum.Value, entityId : Long) : Option[_ <: Entity]

  def getHistory(entityType : KeyEnum.Value, entityId : Long) : mutable.TreeMap[Long, Boolean]
  def getProperties(entityType : KeyEnum.Value, entityId : Long) : mutable.TreeMap[Long, String]
  def getEntities(entityType : KeyEnum.Value) : Set[Long]
  def getEntitiesObjs(entityType : KeyEnum.Value) : TrieMap[Long, Entity]
  def getAssociatedEdges(entityId : Long) : mutable.Set[Long]

  def addEntity(entityType : KeyEnum.Value, entityId : Long, creationTime : Long)
  def addState(entityType : KeyEnum.Value, entityId : Long, timestamp : Long, value : Boolean)
  def addProperty(entityType : KeyEnum.Value, entityId : Long, key : String, timestamp : Long, value : String)

  def addAssociatedVertex(vertexId : Long, edgeId : Long)

}

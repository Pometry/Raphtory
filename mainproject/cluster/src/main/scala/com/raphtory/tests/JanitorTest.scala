package com.raphtory.tests

import ch.qos.logback.classic.Level
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import com.mongodb.casbah.Imports.{$addToSet, _}
import com.mongodb.casbah.MongoConnection
import com.raphtory.core.actors.partitionmanager.MongoFactory
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
object JanitorTest extends App{
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val timeWindow = 1 //timeWindow set in seconds
  val timeWindowMils = timeWindow * 1000




  val vertex = new Vertex(1,1,1,true,false)
  vertex revive(2)
  vertex revive(3)
  vertex revive(4)
  vertex kill(5)
  vertex revive(6)
  vertex +(1,"prop","val1")
  vertex +(2,"prop","val2")
  vertex +(3,"prop","val2")
  vertex +(4,"prop","val3")
  vertex +(5,"prop","val3")
  vertex +(6,"prop","val3")
  vertex +(7,"prop","val3")

  vertex +(1,"prop2","val1")
  vertex +(2,"prop2","val2")
  vertex +(3,"prop2","val2")
  vertex +(4,"prop2","val3")
  vertex +(5,"prop2","val3")
  vertex +(6,"prop2","val3")
  vertex +(7,"prop2","val3")



  vertex addAssociatedEdge new Edge(1,1,1,2,true,false)
  vertex addAssociatedEdge new Edge(1,2,1,3,true,false)
  vertex addAssociatedEdge new Edge(1,3,1,4,true,false)
  vertex addAssociatedEdge new Edge(1,4,1,5,true,false)

  MongoFactory.vertex2Mongo(vertex,cutOff)
  for(edge <- vertex.associatedEdges){
    MongoFactory.edge2Mongo(edge,cutOff)
  }
  MongoFactory.flushBatch()


  vertex kill(7)
  vertex revive(8)
  vertex +(8,"prop3","dave")
  vertex +(9,"prop3","bob")
  vertex addAssociatedEdge new Edge(1,6,1,7,true,false)
//  println(vertex.equals(EntityStorage.retrieveVertex(vertex.getId.toInt)))
//  val retrieved = EntityStorage.retrieveVertex(vertex.getId.toInt)
 MongoFactory.vertex2Mongo(vertex,cutOff)
  for(edge <- vertex.associatedEdges){
    MongoFactory.edge2Mongo(edge,cutOff)
  }
 MongoFactory.flushBatch()
 // Thread.sleep(1000)
//println(MongoFactory.retrieveVertexRaw(1))
  //println(EntityStorage.retrieveVertex(vertex.getId.toInt))
   println(vertex.equals(EntityStorage.retrieveVertex(vertex.getId.toInt)))
  //println(MongoFactory.retrieveEdge(new Edge(1,1,1,2,true,false).getId))
  //println(MongoFactory.retrieveEdgeRaw(new Edge(1,1,1,2,true,false).getId))


  def cutOff = System.currentTimeMillis()+1000


}



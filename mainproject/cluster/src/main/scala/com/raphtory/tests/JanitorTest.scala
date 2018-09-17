package com.raphtory.tests

import ch.qos.logback.classic.Level
import com.outworkers.phantom.dsl.KeySpace
import com.raphtory.core.actors.partitionmanager.Archivist.{RaphtoryDB, VertexHistoryPoint}
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import org.slf4j.LoggerFactory

object JanitorTest extends App{
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val timeWindow = 1 //timeWindow set in seconds
  val timeWindowMils = timeWindow * 1000
  import scala.concurrent.ExecutionContext.Implicits.global



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

  RaphtoryDB.vertexHistory.saveNew(vertex.getId,vertex.oldestPoint.get,vertex.compressAndReturnOldHistory(cutOff))


  vertex kill(7)
  vertex revive(8)
  vertex +(8,"prop3","dave")
  vertex +(9,"prop3","bob")
  vertex addAssociatedEdge new Edge(1,6,1,7,true,false)

  RaphtoryDB.vertexHistory.save(vertex.getId,vertex.compressAndReturnOldHistory(cutOff))
  val fut = RaphtoryDB.vertexHistory.allVertexHistory(vertex.getId)

  println(fut.onComplete(f => println(f.get)))

  //    //RaphtoryDB.vertexHistory.save(i.toLong,i.toLong,true).isCompleted
 //println(System.currentTimeMillis()-time2)
//    //println(RaphtoryDB.connector.session.execute("select * from raphtory.vertexhistory ;").one())
//
  def cutOff = System.currentTimeMillis()+1000



}


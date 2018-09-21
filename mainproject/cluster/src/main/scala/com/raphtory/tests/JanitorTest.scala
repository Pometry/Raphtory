package com.raphtory.tests

import ch.qos.logback.classic.Level
import com.outworkers.phantom.dsl.KeySpace
import com.raphtory.core.actors.partitionmanager.Archivist.{RaphtoryDB, VertexHistoryPoint}
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import com.raphtory.core.utils.exceptions.EntityRemovedAtTimeException
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

object JanitorTest extends App{
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val timeWindow = 1 //timeWindow set in seconds
  val timeWindowMils = timeWindow * 1000
  import scala.concurrent.ExecutionContext.Implicits.global

  RaphtoryDB.vertexHistory.createTable()
  RaphtoryDB.edgeHistory.createTable()
  RaphtoryDB.vertexPropertyHistory.createTable()
  RaphtoryDB.edgePropertyHistory.createTable()

  RaphtoryDB.vertexHistory.clear()
  RaphtoryDB.edgeHistory.clear()
  RaphtoryDB.vertexPropertyHistory.clear()
  RaphtoryDB.edgePropertyHistory.clear()


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
  vertex addAssociatedEdge new Edge(1,3,1,4,false,false)
  vertex addAssociatedEdge new Edge(1,4,1,5,true,false)
  vertex addAssociatedEdge new Edge(1,1,2,1,true,false)
  vertex addAssociatedEdge new Edge(1,2,3,1,true,false)

  for(edge <- vertex.associatedEdges.values){
    edge + (3,"prop","testValue")
  }


  RaphtoryDB.vertexHistory.saveNew(vertex.getId,vertex.oldestPoint.get,vertex.compressAndReturnOldHistory(cutOff))
  vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.saveNew(vertex.getId,prop._1,vertex.oldestPoint.get,prop._2.compressAndReturnOldHistory(cutOff)))
  saveEdges(vertex)

  vertex kill(7)
  vertex revive(8)
  vertex +(8,"prop2","dave")
  vertex +(9,"prop","bob")
  vertex addAssociatedEdge new Edge(1,6,1,7,true,false)
  for(edge <- vertex.associatedEdges.values){
    edge + (4,"prop","testValue2")
  }
  RaphtoryDB.vertexHistory.save(vertex.getId,vertex.compressAndReturnOldHistory(cutOff))
  vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.save(vertex.getId,prop._1,prop._2.compressAndReturnOldHistory(cutOff)))
  saveEdges(vertex)

  retrieveVertex(1,6)



  def saveEdges(vertex:Vertex) ={
    for(edge <- vertex.associatedEdges.values){
      // RaphtoryDB.edgeHistory.saveNew(edge.getSrcId,edge.getDstId,edge.oldestPoint.get,false,edge.compressAndReturnOldHistory(cutOff))
      if(edge.beenSaved()) {
        RaphtoryDB.edgeHistory.save(edge.getSrcId, edge.getDstId, edge.compressAndReturnOldHistory(cutOff))
        for(property<- edge.properties)
          RaphtoryDB.edgePropertyHistory.save(edge.getSrcId,edge.getDstId,property._1,property._2.compressAndReturnOldHistory(cutOff))
      }
      else {
        RaphtoryDB.edgeHistory.saveNew(edge.getSrcId, edge.getDstId, edge.oldestPoint.get, false, edge.compressAndReturnOldHistory(cutOff))
        for (property <- edge.properties)
          RaphtoryDB.edgePropertyHistory.saveNew(edge.getSrcId, edge.getDstId, property._1, edge.oldestPoint.get, false, property._2.compressAndReturnOldHistory(cutOff))
      }
    }

  }


  def retrieveVertex(id:Long,time:Long)={

    val x = for {
      vertexHistory <- RaphtoryDB.vertexHistory.allVertexHistory(id)
      vertexPropertyHistory <- RaphtoryDB.vertexPropertyHistory.allPropertyHistory(id)
      incomingedgehistory <- RaphtoryDB.edgeHistory.allIncomingHistory(id.toInt)
      incomingedgePropertyHistory <- RaphtoryDB.edgePropertyHistory.allIncomingHistory(id.toInt)
      outgoingedgehistory <- RaphtoryDB.edgeHistory.allOutgoingHistory(id.toInt)
      outgoingedgePropertyHistory <- RaphtoryDB.edgePropertyHistory.allOutgoingHistory(id.toInt)
    } yield {
      val vertex = Vertex(vertexHistory.head,time)
      if(!vertex.previousState.head._2)
        throw EntityRemovedAtTimeException(vertex.getId)
      for(property <-  vertexPropertyHistory){
        vertex.addSavedProperty(property,time)
      }
      for(edgepoint <- incomingedgehistory){
        try{vertex.addAssociatedEdge(Edge(edgepoint,time))}
        catch {case e:EntityRemovedAtTimeException => println(edgepoint.dst + "bust") } // edge was not alive at this point in time
      }
      for(edgepropertypoint <- incomingedgePropertyHistory){
        vertex.associatedEdges.get(Utils.getEdgeIndex(edgepropertypoint.src,edgepropertypoint.dst)) match {
          case Some(edge) =>edge.addSavedProperty(edgepropertypoint,time)
          case None => //it was false at given point in time
        }
      }
      for(edgepoint <- outgoingedgehistory){
        try{vertex.addAssociatedEdge(Edge(edgepoint,time))}
        catch {case e:EntityRemovedAtTimeException => println(edgepoint.dst + "bust") } // edge was not alive at this point in time
      }
      for(edgepropertypoint <- outgoingedgePropertyHistory){
        vertex.associatedEdges.get(Utils.getEdgeIndex(edgepropertypoint.src,edgepropertypoint.dst)) match {
          case Some(edge) =>edge.addSavedProperty(edgepropertypoint,time)
          case None => //it was false at given point in time
        }
      }
      vertex
    }

    x.onComplete(p => p match {
      case Success(v)=> println(v)//EntityStorage.vertices.put(v.vertexId,v)
      case Failure(e) => println(e) //do nothing
    })

  }




  //    //RaphtoryDB.vertexHistory.save(i.toLong,i.toLong,true).isCompleted
 //println(System.currentTimeMillis()-time2)
//    //println(RaphtoryDB.connector.session.execute("select * from raphtory.vertexhistory ;").one())
//
  def cutOff = System.currentTimeMillis()+1000



}


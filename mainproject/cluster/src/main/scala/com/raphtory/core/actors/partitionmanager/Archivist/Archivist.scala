package com.raphtory.core.actors.partitionmanager.Archivist

import java.util.concurrent.Executors

import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDB}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration._
//TODO decide how to do shrinking window as graph expands
//TODO work out general cutoff function
//TODO don't resave history
//TODO fix edges
//TODO implement temporal/spacial profiles (future)
//TODO join historian to cluster

class Archivist(maximumHistory:Int, compressionWindow:Int, maximumMem:Double) extends RaphtoryActor {

//  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val runtime = Runtime.getRuntime

  val maximumHistoryMils = maximumHistory * 60000 //set in minutes
  val compressionWindowMils = compressionWindow * 1000 //set in seconds

  var lastSaved : Long = 0
  var newLastSaved : Long = 0
  var canArchiveFlag = false
  var lockerCounter = 0

  lazy val maxThreads = 12

  lazy implicit val scheduler = {
    val javaService = Executors.newScheduledThreadPool(maxThreads)
    Scheduler(javaService, AlwaysAsyncExecution)
  }

  override def preStart() {
    context.system.scheduler.scheduleOnce(7.seconds, self,"compress")
  }

  override def receive: Receive = {
    case "compress" => compressGraph()
    case "archive"=> archive()
  }


  //COMPRESSION BLOCK
  def cutOff = System.currentTimeMillis() - compressionWindowMils
  def compressGraph() : Unit = {
    if (lockerCounter > 0)
      return
    newLastSaved   = cutOff
    canArchiveFlag = false
    println("Compressing")
    lockerCounter += 2
    Task.eval(compressEdges(EntityStorage.edges)).runAsync.onComplete(_ => compressEnder())
    Task.eval(compressVertices(EntityStorage.vertices)).runAsync.onComplete(_ => compressEnder())
  }

  def compressEdges(map:ParTrieMap[Long,Edge]) = {
    val now = newLastSaved
    val past = lastSaved
    map.foreach(e => saveEdge(e._2))
  }
  def compressVertices(map:ParTrieMap[Int,Vertex]) = {
    val now = newLastSaved
    val past = lastSaved
    map.foreach(v => saveVertex(v._2))
  }

  def compressEnder(): Unit = {
    lockerCounter -= 1
    if (lockerCounter == 0) {
      canArchiveFlag = true
      lastSaved = newLastSaved
      context.system.scheduler.scheduleOnce(5.seconds, self,"archive")
    }
  }


  def saveVertex(vertex:Vertex) = {
    if(vertex.beenSaved()) {
      RaphtoryDB.vertexHistory.save(vertex.getId,vertex.compressAndReturnOldHistory(cutOff))
      vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.save(vertex.getId,prop._1,prop._2.compressAndReturnOldHistory(cutOff)))
    }
    else {
      RaphtoryDB.vertexHistory.saveNew(vertex.getId,vertex.oldestPoint.get,vertex.compressAndReturnOldHistory(cutOff))
      vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.saveNew(vertex.getId,prop._1,vertex.oldestPoint.get,prop._2.compressAndReturnOldHistory(cutOff)))
    }
  }

  def saveEdge(edge:Edge) ={
    if(edge.beenSaved()) {
      RaphtoryDB.edgeHistory.save(edge.getSrcId, edge.getDstId, edge.compressAndReturnOldHistory(cutOff))
      edge.properties.foreach(property => RaphtoryDB.edgePropertyHistory.save(edge.getSrcId,edge.getDstId,property._1,property._2.compressAndReturnOldHistory(cutOff)))

    }
    else {
      RaphtoryDB.edgeHistory.saveNew(edge.getSrcId, edge.getDstId, edge.oldestPoint.get, false, edge.compressAndReturnOldHistory(cutOff))
      edge.properties.foreach(property => RaphtoryDB.edgePropertyHistory.saveNew(edge.getSrcId, edge.getDstId, property._1, edge.oldestPoint.get, false, property._2.compressAndReturnOldHistory(cutOff)))
    }
  }









  //END COMPRESSION BLOCK

  //ARCHIVING BLOCK
  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum

  def checkMaximumHistory(e:Entity, et : KeyEnum.Value) = {
      val (placeholder, allOld) = e.removeAncientHistory(System.currentTimeMillis - maximumHistoryMils)
      if (placeholder) {/*TODO decide what to do with placeholders (future)*/}
      if (allOld) {
        et match {
          case KeyEnum.vertices => EntityStorage.vertices.remove(e.getId.toInt)
          case KeyEnum.edges    => EntityStorage.edges.remove(e.getId)
        }
      }

      for ((propkey, propval) <- e.properties) {
        propval.compressAndReturnOldHistory(System.currentTimeMillis - maximumHistoryMils)
      }
  }

  def archive() : Unit ={
    println("Try to archive")
    if (!canArchiveFlag)
      return
    println("Archiving")
    if(!spaceForExtraHistory) { //first check edges
      for (e <- EntityStorage.edges){
        checkMaximumHistory(e._2, KeyEnum.edges)
      }
    }
    if(!spaceForExtraHistory) { //then check vertices
      for (e <- EntityStorage.vertices){
        checkMaximumHistory(e._2, KeyEnum.vertices)
      }
    }
    context.system.scheduler.scheduleOnce(5.seconds, self,"compress")
  }




}






//def compressJob[T <: AnyVal, U <: Entity](map : ParTrieMap[T, U]) = {
//  //val mapSize = map.size
//  //val taskNumber   = maxThreads * 10
//  val batchedElems = mapSize / taskNumber
//
//  var i : Long = 0
//  while (i < taskNumber) {
//  var j = i * batchedElems
//  while (j < (i + 1) * batchedElems) {
//  compressHistory(map(j.asInstanceOf[T]), newLastSaved, lastSaved)
//  j += 1
//}
//  i += 1
//}
//}
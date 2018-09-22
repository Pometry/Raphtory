package com.raphtory.core.actors.partitionmanager.Archivist

import java.util.concurrent.Executors

import ch.qos.logback.classic.Level
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDB}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration._
//TODO decide how to do shrinking window as graph expands
//TODO work out general cutoff function
//TODO don't resave history
//TODO fix edges
//TODO implement temporal/spacial profiles (future)
//TODO join historian to cluster

class Archivist(maximumMem:Double) extends RaphtoryActor {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
//  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val runtime = Runtime.getRuntime

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
    context.system.scheduler.scheduleOnce(30.seconds, self,"compress")
  }

  override def receive: Receive = {
    case "compress" => compressGraph()
    case "archive"=> archive()
  }

  var compressionPercent = 50
  var archivePercentage = 10
  //COMPRESSION BLOCK
  def toCompress(newestPoint:Long,oldestPoint:Long):Long =  ((newestPoint-oldestPoint) / (100f - compressionPercent)).asInstanceOf[Long]
  def toArchive(newestPoint:Long,oldestPoint:Long):Long =  ((newestPoint-oldestPoint) / (100f - archivePercentage)).asInstanceOf[Long]
  def cutOff(compress:Boolean) = {
    val oldestPoint = EntityStorage.oldestTime
    val newestPoint = EntityStorage.newestTime
    if(oldestPoint != Long.MaxValue)
      if(compress)
        oldestPoint + toCompress(newestPoint,oldestPoint) //oldestpoint + halfway to the newest point == always keep half of in memory stuff compressed
      else
        oldestPoint + toArchive(newestPoint,oldestPoint)
    else
      newestPoint
  }

  def compressGraph() : Unit = {
    if (lockerCounter > 0)
      return
    newLastSaved   = cutOff(true)
    canArchiveFlag = false
    println("Compressing")
    lockerCounter += 2
    Task.eval(compressEdges(EntityStorage.edges)).runAsync.onComplete(_ => compressEnder())
    Task.eval(compressVertices(EntityStorage.vertices)).runAsync.onComplete(_ => compressEnder())
  }


  def compressEdges(map : ParTrieMap[Long, Edge]) = {
    val now = newLastSaved
    val past = lastSaved
    for((k,v) <- map) saveEdge(v,now)
  }

  def compressVertices(map : ParTrieMap[Int, Vertex]) = {
    val now = newLastSaved
    val past = lastSaved
    for((k,v) <- map) saveVertex(v,now)
  }


  def compressEnder(): Unit = {
    lockerCounter -= 1
    if (lockerCounter == 0) {
      canArchiveFlag = true
      lastSaved = newLastSaved
      EntityStorage.lastCompressedAt = lastSaved
      context.system.scheduler.scheduleOnce(5.seconds, self,"archive")
    }
  }

  def saveVertex(vertex:Vertex,cutOff:Long) = {
    if(vertex.beenSaved()) {
      RaphtoryDB.vertexHistory.save(vertex.getId,vertex.compressAndReturnOldHistory(cutOff))
      vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.save(vertex.getId,prop._1,prop._2.compressAndReturnOldHistory(cutOff)))
    }
    else {
      RaphtoryDB.vertexHistory.saveNew(vertex.getId,vertex.oldestPoint.get,vertex.compressAndReturnOldHistory(cutOff))
      vertex.properties.foreach(prop => RaphtoryDB.vertexPropertyHistory.saveNew(vertex.getId,prop._1,vertex.oldestPoint.get,prop._2.compressAndReturnOldHistory(cutOff)))
    }
  }


  def saveEdge(edge:Edge,cutOff:Long) ={
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

  def archive() : Unit ={
    println("Try to archive")
    if (!canArchiveFlag)
      return
    println("Archiving")
    if(!spaceForExtraHistory) {
      val removalPoint = cutOff(false)
      for (e <- EntityStorage.edges) {
        checkMaximumHistory(e._2, KeyEnum.edges,removalPoint)
        for (e <- EntityStorage.vertices) {
          checkMaximumHistory(e._2, KeyEnum.vertices,removalPoint)
        }
      }
      EntityStorage.oldestTime = removalPoint
    }
    context.system.scheduler.scheduleOnce(5.seconds, self,"compress")
  }

  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum

  def checkMaximumHistory(e:Entity, et : KeyEnum.Value,removalPoint:Long) = {
      val (placeholder, allOld) = e.removeAncientHistory(removalPoint)
      if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
      if (allOld.asInstanceOf[Boolean]) {
        et match {
          case KeyEnum.vertices => EntityStorage.vertices.remove(e.getId.toInt)
          case KeyEnum.edges    => EntityStorage.edges.remove(e.getId)
        }
      }
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
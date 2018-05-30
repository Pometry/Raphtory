package com.raphtory.core.actors.partitionmanager

import java.util.concurrent.Executors

import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.graphentities.{Edge, Entity, Vertex}
import com.raphtory.core.storage.{EntitiesStorage, RedisConnector}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.Breaks._
import com.raphtory.core.storage.RedisConnector

import scala.collection.parallel.mutable.ParTrieMap
//TODO decide how to do shrinking window as graph expands
//TODO implement temporal/spacial profiles (future)
//TODO join historian to cluster

class Archivist(maximumHistory:Int, compressionWindow:Int, maximumMem:Double) extends RaphtoryActor {

  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
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
    //context.system.scheduler.schedule(2.seconds,5.seconds, self,"archive")

    context.system.scheduler.scheduleOnce(7.seconds, self,"compress")
  }
  override def receive: Receive = {
    case "archive"=> archive()
    case "compress" => compressGraph()
  }

  def archive() : Unit ={
    println("Try to archive")
    if (!canArchiveFlag)
      return
    println("Archiving")
    if(!spaceForExtraHistory) { //first check edges
      for (e <- EntitiesStorage.edges){
        checkMaximumHistory(e._2, KeyEnum.edges)
      }
    }
    if(!spaceForExtraHistory) { //then check vertices
      for (e <- EntitiesStorage.vertices){
        checkMaximumHistory(e._2, KeyEnum.vertices)
      }
    }
  }

  def compressJob[T <: AnyVal, U <: Entity](map : ParTrieMap[T, U]) = {
    val mapSize = map.size
    val taskNumber   = maxThreads * 10
    val batchedElems = mapSize / taskNumber

    var i : Long = 0
    while (i < taskNumber) {
      var j = i * batchedElems
      while (j < (i + 1) * batchedElems) {
        compressHistory(map(j.asInstanceOf[T]), newLastSaved, lastSaved)
        j += 1
      }
      i += 1
    }
  }

  def compressEnder(): Unit = {
    lockerCounter -= 1
    if (lockerCounter == 0) {
      canArchiveFlag = true
      lastSaved = newLastSaved
      context.system.scheduler.scheduleOnce(30.seconds, self,"compress")
    }
  }

  def compressGraph() : Unit = {
    if (lockerCounter > 0)
      return

    newLastSaved   = cutOff
    canArchiveFlag = false
    println("Compressing")

    lockerCounter += 2
    Task.eval(compressJob[Long, Edge](EntitiesStorage.edges)).runAsync.onComplete(_ => compressEnder())
    Task.eval(compressJob[Int, Vertex](EntitiesStorage.vertices)).runAsync.onComplete(_ => compressEnder())
  }

  def checkMaximumHistory(e:Entity, et : KeyEnum.Value) = {
      val (placeholder, allOld, ancientHistory) = e.returnAncientHistory(System.currentTimeMillis - maximumHistoryMils)
      if (placeholder) {
        //TODO decide what to do with placeholders (future)
      }
      if (allOld) {
        et match {
          case KeyEnum.vertices => EntitiesStorage.vertices.remove(e.getId.toInt)
          case KeyEnum.edges    => EntitiesStorage.edges.remove(e.getId)
        }
      }

      for ((propkey, propval) <- e.properties) {
        propval.removeAndReturnOldHistory(System.currentTimeMillis - maximumHistoryMils)
      }
  }

  def compressHistory(e:Entity, now : Long, past : Long) ={
    val compressedHistory = e.compressAndReturnOldHistory(now)
    if(compressedHistory.nonEmpty){
      //TODO  decide if compressed history is rejoined
      var entityType: KeyEnum.Value = null
      var entityId: Long = 0
      if (e.isInstanceOf[Vertex])
        entityType = KeyEnum.vertices
      else
        entityType = KeyEnum.edges
      saveToRedis(compressedHistory, entityType, e.getId, past, e)
      savePropertiesToRedis(e, past)
    }
  }

  def cutOff = System.currentTimeMillis() - compressionWindowMils

  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum


  def saveToRedis(compressedHistory : mutable.TreeMap[Long, Boolean], entityType : KeyEnum.Value, entityId : Long, pastCheckpoint : Long, e :Entity) = {
    RedisConnector.addEntity(entityType, entityId, e.creationTime)
    for ((k,v) <- compressedHistory) {
      if (k > pastCheckpoint)
        RedisConnector.addState(entityType, entityId,k, v)
    }
  }

  def savePropertiesToRedis(e : Entity, pastCheckpoint : Long) = {
    val properties = e.properties
    var entityType = KeyEnum.edges
    val id         = e.getId
    if (e.isInstanceOf[Vertex])
        entityType = KeyEnum.vertices

    properties.foreach(el => {
      val propValue = el._2
      val propName  = el._1
      propValue.previousState.foreach(h => {
        if (h._1 > pastCheckpoint)
          RedisConnector.addProperty(entityType, id, propName, h._1, h._2)
        else break
      })
    })
  }
}

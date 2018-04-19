package com.raphtory.Actors.RaphtoryActors

import com.raphtory.GraphEntities.{EntitiesStorage, Entity, Vertex}
import com.raphtory.Storage.RedisConnector
import com.raphtory.utils.KeyEnum
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

import scala.collection.mutable
import util.control.Breaks._

//TODO decide how to do shrinking window as graph expands
//TODO implement temporal/spacial profiles (future)
//TODO join historian to cluster

class Historian(maximumHistory:Int,compressionWindow:Int,maximumMem:Double) extends RaphtoryActor {

  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val runtime = Runtime.getRuntime

  val maximumHistoryMils = maximumHistory * 60000 //set in minutes
  val compressionWindowMils = compressionWindow * 1000 //set in seconds

  var lastSaved : Long = 0
  var newLastSaved : Long = 0
  var canArchiveFlag = false
  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(100))
  override def receive: Receive = {
    case "archive"=> archive()
    case "compress" => compressGraph()
  }

  def archive() : Unit ={
    if (!canArchiveFlag)
      return
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

  def compressGraph() = {
    newLastSaved = cutOff
    for (e <- EntitiesStorage.edges){
      compressHistory(e._2, newLastSaved, lastSaved)
    }
    for (e <- EntitiesStorage.vertices){
      compressHistory(e._2, newLastSaved, lastSaved)
    }
    canArchiveFlag = true
    lastSaved = newLastSaved
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
      Task(saveToRedis(compressedHistory, entityType, e.getId, past)).fork.runAsync
      Task(savePropertiesToRedis(e, past)).fork.runAsync
      e.rejoinHistory(compressedHistory)
    }
  }

  def cutOff = System.currentTimeMillis() - compressionWindowMils

  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum


  def saveToRedis(compressedHistory : mutable.TreeMap[Long, Boolean], entityType : KeyEnum.Value, entityId : Long, pastCheckpoint : Long) = {
    compressedHistory.foreach(h => {
      if (h._1 > pastCheckpoint)
        Task.eval(RedisConnector.addState(entityType, entityId, h._1, h._2)).fork.runAsync
      else break
    })
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
          Task.eval(RedisConnector.addProperty(entityType, id, propName, h._1, h._2)).fork.runAsync
        else break
      })
    })
  }
}

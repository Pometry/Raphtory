package com.raphtory.core.actors.partitionmanager

import java.util.concurrent.Executors

import ch.qos.logback.classic.Level
import com.mongodb.casbah.MongoConnection
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.graphentities.{Edge, Entity, Property, Vertex}
import com.raphtory.core.storage.{EntitiesStorage, RedisConnector}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import com.mongodb.casbah.Imports.{$addToSet, _}
import com.mongodb.casbah.MongoConnection

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.Breaks._
import com.raphtory.core.storage.RedisConnector
import org.slf4j.LoggerFactory

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
    Task.eval(compressJob[Long, Edge](EntitiesStorage.edges)).runAsync.onComplete(_ => compressEnder())
    Task.eval(compressJob[Int, Vertex](EntitiesStorage.vertices)).runAsync.onComplete(_ => compressEnder())
  }

  def compressJob[T <: AnyVal, U <: Entity](map : ParTrieMap[T, U]) = for((k,v) <- map) compressHistory(v, newLastSaved, lastSaved)

  def compressHistory(e:Entity, now : Long, past : Long) ={
    val compressedHistory = e.compressAndReturnOldHistory(now)
    if(compressedHistory.nonEmpty){
      val entityType = if (e.isInstanceOf[Vertex]) KeyEnum.vertices else KeyEnum.edges
      //saveToRedis(compressedHistory, entityType, e.getId, past, e)

    }
    for ((id,property) <- e.properties){
      val oldHistory = property.compressAndReturnOldHistory(now)
      // savePropertiesToRedis(e, past)
      //store offline
    }
  }

  def compressEnder(): Unit = {
    lockerCounter -= 1
    if (lockerCounter == 0) {
      canArchiveFlag = true
      lastSaved = newLastSaved
      context.system.scheduler.scheduleOnce(5.seconds, self,"archive")
    }
  }
  //END COMPRESSION BLOCK

  //ARCHIVING BLOCK
  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum
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
    context.system.scheduler.scheduleOnce(5.seconds, self,"compress")
  }

  def checkMaximumHistory(e:Entity, et : KeyEnum.Value) = {
      val (placeholder, allOld, ancientHistory) = e.returnAncientHistory(System.currentTimeMillis - maximumHistoryMils)
      if (placeholder) {/*TODO decide what to do with placeholders (future)*/}
      if (allOld) {
        et match {
          case KeyEnum.vertices => EntitiesStorage.vertices.remove(e.getId.toInt)
          case KeyEnum.edges    => EntitiesStorage.edges.remove(e.getId)
        }
      }

      for ((propkey, propval) <- e.properties) {
        propval.compressAndReturnOldHistory(System.currentTimeMillis - maximumHistoryMils)
      }
  }





}

object MongoFactory {
  private val DATABASE = "raphtory"
  val connection = MongoConnection()
  val edges = connection(DATABASE)("edges")
  val vertices = connection(DATABASE)("vertices")
  private val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  def entity2Mongo(entity:Entity)={
    if(entity beenSaved()){
      update(entity)
    }
    else{
      newEntity(entity)
    }

  }

  def update(entity: Entity) ={
    val history = convertHistoryUpdate(entity.compressAndReturnOldHistory(cutOff))
    //MongoFactory.vertices.update(DBObject("_id" -> entity.getId), $addToSet("history") $each(history: _*))
    //MongoFactory.vertices.find(MongoDBObject("_id" -> 1)) .updateOne($addToSet("history") $each(history: _*))
    val builder = MongoFactory.vertices.initializeOrderedBulkOperation
    val dbEntity = builder.find(MongoDBObject("_id" -> entity.getId))
    dbEntity.updateOne($addToSet("history") $each(history:_*))
    for((key,property) <- entity.properties){
      val entityHistory = convertHistoryUpdate(property.compressAndReturnOldHistory(cutOff))
      println(entityHistory)
      if(history.nonEmpty)
        dbEntity.updateOne($addToSet(s"properties.$key") $each(entityHistory:_*))
    }

    val result = builder.execute()
  }

  def newEntity(entity:Entity) ={
    val history = entity.compressAndReturnOldHistory(cutOff)
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> entity.getId
    builder += "oldestPoint" -> entity.oldestPoint.get
    builder += "history" -> convertHistory(history)
    builder += "properties" -> convertProperties(entity.properties)
    MongoFactory.vertices.save(builder.result())
  }

  def convertHistory[b <: Any](history:mutable.TreeMap[Long,b]):MongoDBList ={
    val builder = MongoDBList.newBuilder
    for((k,v) <-history)
      if(v.isInstanceOf[String])
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[String])
      else
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[Boolean])

    builder.result
  }
  def convertHistoryUpdate[b <: Any](history:mutable.TreeMap[Long,b]):List[MongoDBObject] ={
    val builder = mutable.ListBuffer[MongoDBObject]()
    for((k,v) <-history)
      if(v.isInstanceOf[String])
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[String])
      else
        builder += MongoDBObject("time"->k,"value"->v.asInstanceOf[Boolean])

    builder.toList
  }

  def convertProperties(properties: ParTrieMap[String,Property]):MongoDBObject = {
    val builder = MongoDBObject.newBuilder
    for ((k, v) <- properties) {
      builder += k -> convertHistory(v.compressAndReturnOldHistory(cutOff))
    }
    builder.result
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
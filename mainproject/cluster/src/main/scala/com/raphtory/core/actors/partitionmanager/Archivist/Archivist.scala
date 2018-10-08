package com.raphtory.core.actors.partitionmanager.Archivist

import java.util.concurrent.Executors

import ch.qos.logback.classic.Level
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.graphentities._
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
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
  var archivelockerCounter = 0
  var vertexCompressionTime:Long = 0L
  var edgeCompressionTime:Long = 0L

  var startcompressioms = 0
  val compressions:AtomicInt =  AtomicInt(0)

  val propsRemoved:AtomicInt =  AtomicInt(0)
  val historyRemoved:AtomicInt =  AtomicInt(0)
  val verticesRemoved:AtomicInt =  AtomicInt(0)
  val edgesRemoved:AtomicInt =  AtomicInt(0)
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean
  println(s"Archivist compressing = $compressing, Saving = $saving")

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
    case "compressCheck" => compressCheck()
    case "archive"=> archive()
  }

  var compressionPercent = 90f
  var archivePercentage = 10f
  //COMPRESSION BLOCK
  def toCompress(newestPoint:Long,oldestPoint:Long):Long =  (((newestPoint-oldestPoint) / 100f) * compressionPercent).asInstanceOf[Long]
  def toArchive(newestPoint:Long,oldestPoint:Long):Long =  (((newestPoint-oldestPoint) / 100f) * archivePercentage).asInstanceOf[Long]
  def cutOff(compress:Boolean) = {



    val oldestPoint = EntityStorage.oldestTime
    val newestPoint = EntityStorage.newestTime
    println(s" Difference between oldest to newest point ${((newestPoint-oldestPoint)/1000)}, ${(toCompress(newestPoint,oldestPoint))/1000} seconds compressed")

    if(oldestPoint != Long.MaxValue) {
      if (compress) {
        oldestPoint + toCompress(newestPoint, oldestPoint) //oldestpoint + halfway to the newest point == always keep half of in memory stuff compressed

      }
      else
        oldestPoint + toArchive(newestPoint, oldestPoint)
    }
    else
      newestPoint
  }

  def compressGraph() : Unit = {
    if (lockerCounter > 0) {
      context.system.scheduler.scheduleOnce(5.seconds, self,"archive")
      return
    }
    newLastSaved   = cutOff(true)
    if(compressing) {
      canArchiveFlag = false
      println("Compressing")
      lockerCounter += 2
      vertexCompressionTime = System.currentTimeMillis()
      edgeCompressionTime = vertexCompressionTime
      Task.eval(compressEdges(EntityStorage.edges)).runAsync.onComplete(_ => compressEnder("edge"))
      Task.eval(compressVertices(EntityStorage.vertices)).runAsync.onComplete(_ => compressEnder("vertex"))
    }
    else {
      canArchiveFlag=true
      context.system.scheduler.scheduleOnce(5.seconds, self, "archive")
    }
  }


  def compressEdges(map : ParTrieMap[Long, Edge]) = {
    val now = newLastSaved
    val past = lastSaved
    println(map.size)
    for((k,v) <- map) {
      startcompressioms +=1
      Future {
        saveEdge(v, now)
      }.onComplete(p => p match {
        case Success(e) => compressions.increment()
        case Failure(e) => {println("problem"+e); compressions.increment()}
      })
    }
  }

  def compressVertices(map : ParTrieMap[Int, Vertex]) = {
    val now = newLastSaved
    val past = lastSaved
    for((k,v) <- map) {
      startcompressioms +=1
      Future{saveVertex(v,now)}.onComplete(p=>p match{
        case Success(e) => compressions.increment()
        case Failure(e) => {println("problem"+e); compressions.increment()}
      })
    }
  }


  def compressEnder(name:String): Unit = {
    lockerCounter -= 1
    if(name equals("edge")){
      println(s"finished $name compressing in ${(System.currentTimeMillis()-edgeCompressionTime)/1000} seconds")
    }
    if(name equals("vertex")){
      println(s"finished $name compressing in ${(System.currentTimeMillis()-vertexCompressionTime)/1000}seconds")
    }

    if (lockerCounter == 0) {
      context.system.scheduler.scheduleOnce(10.millisecond, self, "compressCheck")

    }
  }

  def compressCheck():Unit = {
    if(startcompressioms<= compressions.get) {
      startcompressioms = 0
      compressions.set(0)
      println("finished compressing")
      canArchiveFlag = true
      lastSaved = newLastSaved
      EntityStorage.lastCompressedAt = lastSaved
      context.system.scheduler.scheduleOnce(5.seconds, self, "archive")
    }
    else{
      context.system.scheduler.scheduleOnce(10.millisecond, self, "compressCheck")
    }
  }

  def saveVertex(vertex:Vertex,cutOff:Long) = {
    val history = vertex.compressAndReturnOldHistory(cutOff)
    if(saving) { //if we are saving data to cassandra
      if (history.size > 0) {
        RaphtoryDBWrite.vertexHistory.save(vertex.getId, history)
      }
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
        }
      })
    }
  }


  def saveEdge(edge:Edge,cutOff:Long) ={
    val history = edge.compressAndReturnOldHistory(cutOff)
    if(saving) {
      if(history.size > 0) {
        RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)

      }

      edge.properties.foreach(property => {
        val propHistory = property._2.compressAndReturnOldHistory(cutOff)
        if(propHistory.size > 0) {
          RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)

        }
      })
    }
  }








  //END COMPRESSION BLOCK

  //ARCHIVING BLOCK

  def archive() : Unit = {
    println("Try to archive")
    if((archivelockerCounter == 0)&&(canArchiveFlag)&&(!spaceForExtraHistory)) {
      println("Archiving")
      val removalPoint = cutOff(false)
      archivelockerCounter += 2
      Task.eval(archiveEdges(EntityStorage.edges, removalPoint)).runAsync.onComplete(_ => archiveEnder(removalPoint))
      Task.eval(archiveVertices(EntityStorage.vertices, removalPoint)).runAsync.onComplete(_ => archiveEnder(removalPoint))
    }
    else {
      context.system.scheduler.scheduleOnce(5.seconds, self,"compress")
    }
  }

  def archiveEdges(map : ParTrieMap[Long, Edge],removalPoint:Long) = {
    for (e <- EntityStorage.edges) {
      checkMaximumHistory(e._2, KeyEnum.edges,removalPoint)
    }
  }

  def archiveVertices(map : ParTrieMap[Int, Vertex],removalPoint:Long) = {
    for (e <- EntityStorage.vertices) {
      checkMaximumHistory(e._2, KeyEnum.vertices,removalPoint)
    }
  }


  def archiveEnder(removalPoint:Long): Unit = {
    archivelockerCounter -= 1
    if (archivelockerCounter == 0) {
      EntityStorage.oldestTime = removalPoint
      println(s"Removed ${verticesRemoved.get} vertices ${edgesRemoved.get} edges ${historyRemoved.get} history points and ${propsRemoved.get} property points")
      verticesRemoved.set(0)
      edgesRemoved.set(0)
      historyRemoved.set(0)
      propsRemoved.set(0)
      // if(!spaceForExtraHistory) {
      //   archive()
      // }
      context.system.scheduler.scheduleOnce(10.millisecond, self, "archive") //restart archive to check if there is now enough space

    }
  }


  def spaceForExtraHistory = {
    val total = runtime.freeMemory/runtime.totalMemory().asInstanceOf[Float]
    //println(s"max ${runtime.maxMemory()} total ${runtime.totalMemory()} diff ${runtime.maxMemory()-runtime.totalMemory()} ")
    println(s"Memory usage at $total%")
    if(total < (1-maximumMem)) true else false
  } //check if used memory less than set maximum

  def checkMaximumHistory(e:Entity, et : KeyEnum.Value,removalPoint:Long) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    propsRemoved.add(propremoved.asInstanceOf[Int])
    historyRemoved.add(removed.asInstanceOf[Int])
    if (allOld.asInstanceOf[Boolean]) {
      et match {
        case KeyEnum.vertices => {
          EntityStorage.vertices.remove(e.getId.toInt)
          verticesRemoved.add(1)
        }
        case KeyEnum.edges    => {
          EntityStorage.edges.remove(e.getId)
          edgesRemoved.add(1)
        }
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
package com.raphtory.core.actors.partitionmanager.Archivist

import java.util.concurrent.Executors

import akka.actor.Props
import ch.qos.logback.classic.Level
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.partitionmanager.Archivist.Helpers.{ArchivingSlave, CompressionSlave}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities._
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global
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
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean
  println(s"Archivist compressing = $compressing, Saving = $saving")
  //Turn logging off
  //val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  //root.setLevel(Level.ERROR)
  //get the runtime for memory usage
  val runtime = Runtime.getRuntime

  //times to track how long compression and archiving takes
  var vertexCompressionTime:Long = 0L
  var edgeCompressionTime:Long = 0L
  var totalCompressionTime:Long = 0L

  var vertexArchiveTime:Long =0L
  var edgeArchiveTime:Long = 0L
  var totalArchiveTime:Long = 0L
  //var for the new oldest point after archiving
  var removePointGlobal:Long = 0L

  val edgeCompressor =  context.actorOf(Props[CompressionSlave],"edgecompressor");
  val vertexCompressor =  context.actorOf(Props[CompressionSlave],"vertexcompressor");
  var vetexCompressionFinished = false
  var edgeCompressionFinished = false
  val archiver = context.actorOf(Props[ArchivingSlave],"archiver")

  var compressionPercent = 90f
  var archivePercentage = 30f

  var lastSaved : Long = 0
  var newLastSaved : Long = 0


  override def preStart() {
    edgeCompressor ! SetupSlave(5)
    vertexCompressor ! SetupSlave(5)
    context.system.scheduler.scheduleOnce(30.seconds, self,"compress")
  }

  override def receive: Receive = {
    case "compress" => compressGraph()
    //case "archive"=> archive()
    case FinishedEdgeCompression(total) => compressEnder("edge")
    case FinishedVertexCompression(total) => compressEnder("vertex")
  }

  def compressGraph() : Unit = {
    println("Compressing")
    newLastSaved = cutOff(true)
    edgeCompressor ! CompressEdges(newLastSaved)
    vertexCompressor ! CompressVertices(newLastSaved)

    vertexCompressionTime = System.currentTimeMillis()
    edgeCompressionTime = vertexCompressionTime
    totalCompressionTime = vertexCompressionTime
  }


  def compressEnder(name:String): Unit = {
    if(name equals("edge")){
      println(s"finished $name compressing in ${(System.currentTimeMillis()-edgeCompressionTime)/1000} seconds")
      edgeCompressionFinished = true
    }
    if(name equals("vertex")){
      println(s"finished $name compressing in ${(System.currentTimeMillis()-vertexCompressionTime)/1000}seconds")
      vetexCompressionFinished = true
    }

    if(edgeCompressionFinished && vetexCompressionFinished){
      println(s"finished total compression in ${(System.currentTimeMillis()-totalCompressionTime)/1000} seconds")
      lastSaved = newLastSaved
      EntityStorage.lastCompressedAt = lastSaved
      //context.system.scheduler.scheduleOnce(5.millisecond, self, "archive")
      vetexCompressionFinished = false
      edgeCompressionFinished = false
      context.system.scheduler.scheduleOnce(5.seconds, self, "compress")
    }
  }



  //END COMPRESSION BLOCK

  ARCHIVING BLOCK

  def archive() : Unit = {
    println("Try to archive")
    if((archivelockerCounter == 0)&&(canArchiveFlag)&&(!spaceForExtraHistory)) {
      println("Archiving")
      val removalPoint = cutOff(false)
      archivelockerCounter += 2
      vertexArchiveTime = System.currentTimeMillis()
      edgeArchiveTime = vertexArchiveTime
      totalArchiveTime = vertexArchiveTime
      Task.eval(archiveEdges(EntityStorage.edges, removalPoint)).runAsync.onComplete(_ => archiveEnder(removalPoint,"edge"))
      Task.eval(archiveVertices(EntityStorage.vertices, removalPoint)).runAsync.onComplete(_ => archiveEnder(removalPoint,"Vertex"))
    }
    else {
      context.system.scheduler.scheduleOnce(5.seconds, self,"compress")
    }
  }



  def archiveEnder(removalPoint:Long,name:String): Unit = {
    archivelockerCounter -= 1
    if(name equals("edge")){
      println(s"finished $name archiving in ${(System.currentTimeMillis()-edgeArchiveTime)/1000} seconds")
    }
    if(name equals("vertex")){
      println(s"finished $name archiving in ${(System.currentTimeMillis()-vertexArchiveTime)/1000}seconds")
    }

    if (archivelockerCounter == 0) {
      removePointGlobal = removalPoint
      context.system.scheduler.scheduleOnce(5.millisecond, self, "archiveCheck")

    }

  }

  def archiveCheck():Unit = {
    if(startarchive.get <= archiveFinished.get) {
      startarchive.set(0)
      archiveFinished.set(0)
      println(s"finished total archiving in ${(System.currentTimeMillis()-totalArchiveTime)/1000} seconds")
      EntityStorage.oldestTime = removePointGlobal
      println(s"Removed ${verticesRemoved.get} vertices ${edgesRemoved.get} edges ${historyRemoved.get} history points and ${propsRemoved.get} property points")
      verticesRemoved.set(0)
      edgesRemoved.set(0)
      historyRemoved.set(0)
      propsRemoved.set(0)
      System.gc()
      //if(!spaceForExtraHistory) {
      //   archive()
      //}
      context.system.scheduler.scheduleOnce(10.millisecond, self, "compress") //restart archive to check if there is now enough space
    }
    else{
      context.system.scheduler.scheduleOnce(5.millisecond, self, "archiveCheck")
    }

  }




  def spaceForExtraHistory = {
    val factor = 2
    val totalMemory = runtime.maxMemory
    val freeMemory = runtime.freeMemory
    val usedMemory = (totalMemory - freeMemory)
    val total = usedMemory/(totalMemory/factor).asInstanceOf[Float]
    //println(s"max ${runtime.maxMemory()} total ${runtime.totalMemory()} diff ${runtime.maxMemory()-runtime.totalMemory()} ")
    println(s"Memory usage at ${total*100}% of ${totalMemory/(1024*1024*factor)}MB")
    if(total < (1-maximumMem)) true else false
  } //check if used memory less than set maximum

  def toCompress(newestPoint:Long,oldestPoint:Long):Long =  (((newestPoint-oldestPoint) / 100f) * compressionPercent).asInstanceOf[Long]
  def toArchive(newestPoint:Long,oldestPoint:Long):Long =  (((newestPoint-oldestPoint) / 100f) * archivePercentage).asInstanceOf[Long]
  def cutOff(compress:Boolean) = {
    val oldestPoint = EntityStorage.oldestTime
    val newestPoint = EntityStorage.newestTime
    println(s" Difference between oldest $oldestPoint to newest point $newestPoint --- ${((newestPoint-oldestPoint)/1000)}, ${(toCompress(newestPoint,oldestPoint))/1000} seconds compressed")
    if(oldestPoint != Long.MaxValue) {
      if (compress) oldestPoint + toCompress(newestPoint, oldestPoint) //oldestpoint + halfway to the newest point == always keep half of in memory stuff compressed
      else oldestPoint + toArchive(newestPoint, oldestPoint)
    }
    else newestPoint
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


//export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_162.jdk/Contents/Home/
//JAVA_OPTS=-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:+UseParNewGC -Xms10g -Xmx10g -XX:NewRatio=3
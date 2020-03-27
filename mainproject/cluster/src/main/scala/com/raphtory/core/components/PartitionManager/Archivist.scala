package com.raphtory.core.components.PartitionManager

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import ch.qos.logback.classic.Level
import com.raphtory.core.components.PartitionManager.Workers.ArchivistWorker
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import kamon.Kamon
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

//TODO fix edges

class Archivist(maximumMem: Double, workers: ParTrieMap[Int, ActorRef], storages: ParTrieMap[Int, EntityStorage])
        extends Actor {
  val compressing: Boolean = Utils.compressing
  val saving: Boolean      = Utils.persistenceEnabled
  val archiving: Boolean   = Utils.archiving
  val debug                = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean
  if (debug) println(s"Archivist compressing = $compressing, Saving = $saving, Archiving = $archiving")

  //Turn logging off
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  //get the runtime for memory usage
  val runtime = Runtime.getRuntime
  //times to track how long compression and archiving takes
  var totalCompressionTime: Long = 0L
  var totalArchiveTime: Long     = 0L
  // percent of history to be compressed/archived
  var compressionPercent = 90f
  var archivePercentage  = 10f
  // vars for the latest point the graph is saved to
  var lastSaved    = 0L
  var newLastSaved = 0L
  //var for the new oldest point after archiving
  var removePointGlobal: Long = 0L
  var removalPoint: Long      = 0L

  var currentWorker         = 0
  var currentState: Boolean = false

  // children for distribution of compresssion and archiving
  val vertexManager = context
    .actorOf(Props(new ArchivistWorker(workers, storages)).withDispatcher("archivist-dispatcher"), "vertexcompressor");

  val archGauge = Kamon.gauge("raphtory_archivist")

  override def preStart() {
    context.system.scheduler.scheduleOnce(60.seconds, self, "archive") //start the compression process in 20 seconds
  }

  override def receive: Receive = {
    case "archive"                      => archive()
    case FinishedVertexCompression(key) => compressEnder("vertex")
    case FinishedVertexArchiving(key)   => archiveEnder("vertex")
  }

  def archive(): Unit =
    if (archiving) {
      println(s"Currently archiving worker $currentWorker")
      if (spaceForExtraHistory) //check if we need to archive
        if (compressing) {
          if (currentWorker == 0)
            newLastSaved = cutOff(true) //get the cut off boundry for 90% of history in meme
          vertexManager ! CompressVertices(newLastSaved, workerID = currentWorker)
        } else
          context.system.scheduler.scheduleOnce(60.second, self, "archive")
      else {
        if (currentWorker == 0) {
          removalPoint = cutOff(false) // get the cut off for 10% of the compressed history
          removePointGlobal = removalPoint
          newLastSaved = cutOff(true)
        }
        vertexManager ! ArchiveVertices(newLastSaved, removalPoint, workerID = currentWorker)
      }
    }

  def compressEnder(name: String): Unit = {
    if (debug)
      println(
              s"finished total compression in ${(System.currentTimeMillis() - totalCompressionTime) / 1000} seconds"
      ) //report this to the user
    archGauge
      .refine("actor" -> "Archivist", "name" -> "totalCompressionTime")
      .set((System.currentTimeMillis() - totalCompressionTime) / 1000)
    if (currentWorker == 9)
      lastSaved = newLastSaved
    //EntityStorage.lastCompressedAt = lastSaved //update the saved vals so we know where we are compressed up to ??????????????????????????????
    nextWorker()
  }

  def archiveEnder(name: String): Unit = {
    // archGauge.refine("actor" -> "Archivist", "name" -> "edgeHistoryRemoved").set(storage.edgeHistoryDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "edgePropertyRemoved").set(EntityStorage.edgePropertyDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "edgeEdgesRemoved").set(EntityStorage.edgeDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "vertexHistoryRemoved").set(EntityStorage.vertexHistoryDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "vertexPropertyRemoved").set(EntityStorage.vertexPropertyDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "vertexVerticesRemoved").set(EntityStorage.vertexDeletionCount.sum)
    // archGauge.refine("actor" -> "Archivist", "name" -> "totalArchiveTime").set((System.currentTimeMillis()-totalArchiveTime)/1000)
    if (currentWorker == 9)
      //   EntityStorage.oldestTime = removePointGlobal
      System.gc() //suggest a good time to garbage collect
    nextWorker()
    if (debug) println(s"finished total archiving in ${(System.currentTimeMillis() - totalArchiveTime) / 1000} seconds")
  }

  def nextWorker() =
    if (currentWorker == 9) {
      currentWorker = 0
      context.system.scheduler.scheduleOnce(60.second, self, "archive") //start the archiving process
    } else {
      currentWorker += 1
      context.system.scheduler.scheduleOnce(1.millisecond, self, "archive") //start the archiving process
    }

  def spaceForExtraHistory: Boolean = {
    if (currentWorker == 0) {
      val totalMemory = runtime.maxMemory
      val freeMemory  = runtime.freeMemory
      val usedMemory  = (totalMemory - freeMemory)
      val total       = usedMemory / (totalMemory).asInstanceOf[Float]
      //println(s"max ${runtime.maxMemory()} total ${runtime.totalMemory()} diff ${runtime.maxMemory()-runtime.totalMemory()} ")
      println(s"Memory usage at ${total * 100}% of ${totalMemory / (1024 * 1024)}MB")
      archGauge.refine("actor" -> "Archivist", "name" -> "memoryPercentage").set((total * 100).asInstanceOf[Long])
      if (total < (1 - maximumMem)) currentState = true else currentState = false
    }
    currentState
  } //check if used memory less than set maximum

  def toCompress(newestPoint: Long, oldestPoint: Long): Long =
    (((newestPoint - oldestPoint) / 100f) * compressionPercent).asInstanceOf[Long]
  def toArchive(newestPoint: Long, oldestPoint: Long): Long =
    (((newestPoint - oldestPoint) / 100f) * archivePercentage).asInstanceOf[Long]

  def cutOff(compress: Boolean) = {
    val oldestPoint = storages.map(s => s._2.oldestTime).max
    val newestPoint = storages.map(s => s._2.newestTime).max
    setActionTime(compress)
    if (compress)
      println(
              s" Difference between oldest $oldestPoint to newest point $newestPoint --- ${((newestPoint - oldestPoint) / 1000)}, ${(toCompress(newestPoint, oldestPoint)) / 1000} seconds compressed"
      )
    if (oldestPoint != Long.MaxValue)
      if (compress)
        oldestPoint + toCompress(
                newestPoint,
                oldestPoint
        ) //oldestpoint + halfway to the newest point == always keep half of in memory stuff compressed
      else oldestPoint + toArchive(newestPoint, oldestPoint)
    else newestPoint
  }

  def setActionTime(CorA: Boolean) =
    if (CorA) totalCompressionTime = System.currentTimeMillis()
    else totalArchiveTime = System.currentTimeMillis()

}

//export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_162.jdk/Contents/Home/
//JAVA_OPTS=-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:+UseParNewGC -Xms10g -Xmx10g -XX:NewRatio=3

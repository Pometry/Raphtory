package com.raphtory.core.utils

import java.io._
import java.text.SimpleDateFormat

import akka.actor.ActorContext
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.mutable

object Utils {
  val clusterSystemName  = "dockerexp"
  val config             = ConfigFactory.load
  val partitionsTopic    = "/partitionsCount"
  val readersTopic       = "/readers"
  val readersWorkerTopic = "/readerWorkers"
  val liveAnalysisTopic  = "/liveanalysis"

  val totalWorkers = 10 //must be power of 10

  var local: Boolean              = System.getenv().getOrDefault("LOCAL", "false").trim.toBoolean

  def watchDogSelector(context: ActorContext, ip: String) =
    // IP $clusterSystemName@${InetAddress.getByName("watchDog").getHostAddress()}
    context.actorSelection(s"akka.tcp://$ip:${config.getString("settings.bport")}/user/WatchDog")

  def getPartition(ID: Long, managerCount: Int): Int =
    ((ID.abs % (managerCount * totalWorkers)) / totalWorkers).toInt
  def getWorker(ID: Long, managerCount: Int): Int =
    ((ID.abs % (managerCount * totalWorkers)) % totalWorkers).toInt
  //get the partition a vertex is stored in
  def checkDst(dstID: Long, managerCount: Int, managerID: Int): Boolean =
    getPartition(dstID, managerCount) == managerID //check if destination is also local
  def checkWorker(dstID: Long, managerCount: Int, workerID: Int): Boolean =
    getWorker(dstID, managerCount) == workerID //check if destination is also local

  def getManager(srcId: Long, managerCount: Int): String = {
    val mod     = srcId.abs % (managerCount * totalWorkers)
    val manager = mod / totalWorkers
    val worker  = mod % totalWorkers
    s"/user/Manager_${manager}_child_$worker"
  }

  def getReader(srcId: Long, managerCount: Int): String = {
    val mod     = srcId.abs % (managerCount * totalWorkers)
    val manager = mod / totalWorkers
    val worker  = mod % totalWorkers
    s"/user/Manager_${manager}_reader_$worker"
  }
  def getReaderInt(srcId: Long, managerCount: Int): (Long, Long) = {
    val mod     = srcId.abs % (managerCount * totalWorkers)
    val manager = mod / totalWorkers
    val worker  = mod % totalWorkers
    (manager, worker)
  }

  def getAllReaders(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      workers += s"/user/ManagerReader_$i"
    workers.toArray
  }

  def getAllReaderWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/Manager_${i}_reader_$j"
    workers.toArray
  }

  def getAllWriterWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/Manager_${i}_child_$j"
    workers.toArray
  }

  def getAllRouterWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/router/router_${i}_Worker_$j"
    workers.toArray
  }

  def createHistory(history: mutable.TreeMap[Long, Boolean]): String = {
    if (history.size == 0)
      return ""
    var s = "{"
    for ((k, v) <- history)
      s = s + s"$k : $v, "
    s.dropRight(2) + "}"
  }

  def createPropHistory(history: mutable.TreeMap[Long, String]): String = {
    if (history.size == 0)
      return ""
    var s = "{"
    for ((k, v) <- history)
      s = s + s"$k : '${StringEscapeUtils.escapeJava(v.replaceAll("'", ""))}', "
    s.dropRight(2) + "}"
  }

  def nowTimeStamp()                  = new SimpleDateFormat("dd-MM hh:mm:ss").format(System.currentTimeMillis())
  def unixToTimeStamp(unixTime: Long) = new SimpleDateFormat("dd-MM hh:mm:ss").format(unixTime)

  def writeLines(fileName: String, line: String, header: String): Unit = {
    val f = new File(fileName)
    if (!f.exists()) {
      f.createNewFile()
      val file = new FileWriter(fileName, true)
      var bw   = new BufferedWriter(file)
      bw.write(header)
      bw.newLine()
      bw.write(line)
      bw.newLine()
      bw.flush()
    } else {
      val file = new FileWriter(fileName, true)
      var bw   = new BufferedWriter(file)
      bw.write(line)
      bw.newLine()
      bw.flush()
    }

  }

  def IPRegex =
    "\\b(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\b"

}

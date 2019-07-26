package com.raphtory.core.utils

import java.text.SimpleDateFormat

import akka.actor.ActorContext
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.graphentities.{Edge, RemoteEdge, RemotePos}
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

import java.io._
import java.util.Base64
import java.nio.charset.StandardCharsets.UTF_8
object Utils {
  val clusterSystemName = "dockerexp"
  val config            = ConfigFactory.load
  val partitionsTopic   = "/partitionsCount"
  val readersTopic      = "/readers"
  val readersWorkerTopic      = "/readerWorkers"
  val liveAnalysisTopic      = "/liveanalysis"
  val saving: Boolean = System.getenv().getOrDefault("SAVING", "false").trim.toBoolean
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "false").trim.toBoolean
  val archiving : Boolean =  System.getenv().getOrDefault("ARCHIVING", "false").trim.toBoolean
  var windowing        : Boolean =  System.getenv().getOrDefault("WINDOWING", "false").trim.toBoolean

  val analyserMap: TrieMap[String, Analyser] = TrieMap[String, Analyser]()

  def watchDogSelector(context : ActorContext, ip : String) = {
    // IP $clusterSystemName@${InetAddress.getByName("watchDog").getHostAddress()}
    context.actorSelection(s"akka.tcp://$ip:${config.getString("settings.bport")}/user/WatchDog")
  }

  def getPartition(ID:Int, managerCount : Int):Int = {
    (ID.abs % (managerCount *10)) /10
  }
  def getWorker(ID:Int, managerCount : Int):Int = {
    (ID.abs % (managerCount *10)) %10
  }
  //get the partition a vertex is stored in
  def checkDst(dstID:Int, managerCount:Int, managerID:Int):Boolean = getPartition(dstID,managerCount) == managerID //check if destination is also local
  def checkWorker(dstID:Int, managerCount:Int, workerID:Int):Boolean = getWorker(dstID,managerCount) == workerID //check if destination is also local

  def getManager(srcId:Int, managerCount : Int):String = {
    val mod = srcId.abs % (managerCount *10)
    val manager = mod /10
    val worker = mod % 10
    s"/user/Manager_${manager}_child_$worker"
  } //simple srcID hash at the moment
  def getReader(srcId:Int, managerCount : Int):String = {
    val mod = srcId.abs % (managerCount *10)
    val manager = mod /10
    val worker = mod % 10

    s"/user/Manager_${manager}_reader_$worker"
  }

  def getAllReaders(managerCount:Int):Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for(i <- 0 until managerCount)
        workers += s"/user/ManagerReader_${i}"
    workers.toArray
  }

  def getAllReaderWorkers(managerCount:Int):Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for(i <- 0 until managerCount)
      for(j <- 0 until 10)
        workers += s"/user/Manager_${i}_reader_$j"
    workers.toArray
  }
    /**
    * Shifter to get only one Long as Edges indexing
    * @param srcId
    * @param dstId
    * @return
    */
  def getEdgeIndex(srcId : Int, dstId : Int): Long = {
    (srcId.toLong << 32) + dstId
  }

  /**
    * Get lowest (32 bit) part of a Binary string representing two Int numbers (used for Edges indexing) - The dstId of a given edgeId
    * @param index the Long representing the two Int numbers
    * @return the rightmost 32bit as an Int
    */
  def getIndexLO(index : Long) : Int = {
    ((index << 32) >> 32).toInt
  }

  /**
    * Get highest (32 bit) part of a binary string representing two Int number (used for Edges indexing)
    * @param index
    * @return the leftmost 32 bit as an Int
    */
  def getIndexHI(index : Long) : Int = {
    (index >> 32).toInt
  }


  def createHistory(history: mutable.TreeMap[Long, Boolean]):String = {
    if(history.size==0){
      return ""
    }
    var s = "{"
    for((k,v) <- history){
      s = s+ s"$k : $v, "
    }
    s.dropRight(2) + "}"
  }

  def createPropHistory(history: mutable.TreeMap[Long, String]):String = {
    if(history.size==0){
      return ""
    }
    var s = "{"
    for((k,v) <- history){
      s = s+ s"$k : '${StringEscapeUtils.escapeJava(v.replaceAll("'",""))}', "
    }
    s.dropRight(2) + "}"
  }

  def nowTimeStamp()= new SimpleDateFormat("dd-MM hh:mm:ss").format(System.currentTimeMillis())
  def unixToTimeStamp(unixTime:Long) = new SimpleDateFormat("dd-MM hh:mm:ss").format(unixTime)



  def serialise(value: Any): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    new String(
      Base64.getEncoder().encode(stream.toByteArray),
      UTF_8
    )
  }

  def deserialise(str: String): Any = {
    val bytes = Base64.getDecoder().decode(str.getBytes(UTF_8))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }
}

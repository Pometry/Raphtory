package com.raphtory.core.utils

import java.text.SimpleDateFormat

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.graphentities.{Edge, RemoteEdge}
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
  var local        : Boolean =  System.getenv().getOrDefault("LOCAL", "false").trim.toBoolean

  val analyserMap: TrieMap[String, Analyser] = TrieMap[String, Analyser]()

  def watchDogSelector(context : ActorContext, ip : String) = {
    // IP $clusterSystemName@${InetAddress.getByName("watchDog").getHostAddress()}
    context.actorSelection(s"akka.tcp://$ip:${config.getString("settings.bport")}/user/WatchDog")
  }

  def getPartition(ID:Long, managerCount : Int):Int = {
    ((ID.abs % (managerCount *10)) /10).toInt
  }
  def getWorker(ID:Long, managerCount : Int):Int = {
    ((ID.abs % (managerCount *10)) %10).toInt
  }
  //get the partition a vertex is stored in
  def checkDst(dstID:Long, managerCount:Int, managerID:Int):Boolean = getPartition(dstID,managerCount) == managerID //check if destination is also local
  def checkWorker(dstID:Long, managerCount:Int, workerID:Int):Boolean = getWorker(dstID,managerCount) == workerID //check if destination is also local

  def getManager(srcId:Long, managerCount : Int):String = {
    val mod = srcId.abs % (managerCount *10)
    val manager = mod /10
    val worker = mod % 10
    s"/user/Manager_${manager}_child_$worker"
  }

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

  def writeLines(fileName: String , line: String,header:String) : Unit = {
    val f = new File(fileName)
    if(!f.exists()) {
      f.createNewFile()
      val file=new FileWriter(fileName, true)
      var bw =new BufferedWriter(file)
      bw.write(header)
      bw.newLine()
      bw.write(line)
      bw.newLine()
      bw.flush()
    }
    else{
      val file=new FileWriter(fileName, true)
      var bw =new BufferedWriter(file)
      bw.write(line)
      bw.newLine()
      bw.flush()
    }

  }

  def windowOutut(outputFile:String,windowSize: Long) = {
    outputFile.substring(0,outputFile.length-4)+(windowSize/3600000)+".csv"
  }


   object resultNumeric extends Numeric[(Long, Double)] {
    override def plus(x: (Long, Double), y: (Long, Double)) = (x._1 + y._1, x._2 + y._2)
    override def minus(x: (Long, Double), y: (Long, Double)) = (x._1 - y._1, x._2 - y._2)
    override def times(x: (Long, Double), y: (Long, Double)) = (x._1 * y._1, x._2 * y._2)
    override def negate(x: (Long, Double)) = (-x._1, -x._2)
    override def fromInt(x: Int) = (x, x)
    override def toInt(x: (Long, Double)) = x._1.toInt
    override def toLong(x: (Long, Double)) = x._1
    override def toFloat(x: (Long, Double)) = x._2.toFloat
    override def toDouble(x: (Long, Double)) = x._2
    override def compare(x: (Long, Double), y: (Long, Double)) = x._2.compare(y._2)
  }

}

object KeyEnum extends Enumeration {
  val vertices : Value = Value("vertices")
  val edges    : Value = Value("edges")
}

object SubKeyEnum extends Enumeration {
  val history      : Value = Value("history")
  val creationTime : Value = Value("creationTime")
}
object CommandEnum extends Enumeration {
  val edgeAdd : Value = Value("edgeAdd")
  val vertexAdd : Value = Value("vertexAdd")
  val vertexAddWithProperties : Value = Value("vertexAddWithProperties")
  val edgeAddWithProperties : Value = Value("edgeAddWithProperties")
}


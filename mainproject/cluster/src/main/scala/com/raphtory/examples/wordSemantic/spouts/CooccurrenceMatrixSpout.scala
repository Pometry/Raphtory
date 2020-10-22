package com.raphtory.examples.wordSemantic.spouts


import java.io.{BufferedReader, File, FileReader}
import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.spouts.EtherFileReader

import scala.concurrent.duration._
import scala.language.postfixOps

class CooccurrenceMatrixSpout extends SpoutTrait {

  println("Start: " + LocalDateTime.now())
  val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean
  var JUMP = System.getenv().getOrDefault("FILE_SPOUT_BLOCK_SIZE", "100").trim.toInt
  var INCREMENT = System.getenv().getOrDefault("FILE_SPOUT_INCREMENT", "1").trim.toInt
  var TIME = System.getenv().getOrDefault("FILE_SPOUT_TIME", "60").trim.toInt

  var directoryPosition    = 0

  val filesToRead = if(fileName.isEmpty)
    getListOfFiles(directory)
  else
    Array(directory + "/" + fileName)

  var currentFile = fileToArray(directoryPosition)
  val JUMP2 = 20
  var posSlice = 1
  var cline = currentFile.readLine()
  var currentLine = cline.split("\t")
  var filename = filesToRead(directoryPosition) //D-200001_merge_occ
  var time = filename.split('/').last.stripPrefix("D-").stripSuffix("_merge_occ").toLong * 1000000000L
  var cnt = time + 1



  override protected def ProcessSpoutTask(message: Any): Unit = message match {
  case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineSlice")

  case "nextLineSlice" => nextLineSlice()
  case "nextLineBLock" => nextLineBlock()
  case "nextFile" => nextFile()
  case _ => println("message not recognized!")
}
  def nextLineSlice() = {
    try {
      if (posSlice <= currentLine.length-1) {
        val head = currentLine(0)
        for (i<- 1 to Set(JUMP, currentLine.length-posSlice/JUMP2).min) {
          val currentSlice = currentLine.slice(posSlice, posSlice + JUMP2)
          sendTuple(cnt.toString + ' ' + head + "\t" + currentSlice.mkString("\t"))
          posSlice += JUMP2
        }
        AllocateSpoutTask(Duration(1, MILLISECONDS), "nextLineSlice")
      }
      else {
        posSlice = 1
        AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
      }
    }catch {
      case e: Exception => println(e,  posSlice)
    }
  }

  def nextLineBlock() = {
    try {
      cnt += 1
      cline = currentFile.readLine()
      currentLine = cline.split("\t")
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineSlice")
      }
    catch {
      case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
    }
  }

  def nextFile() = {
    directoryPosition += 1
    if (filesToRead.length > directoryPosition) {
      currentFile = fileToArray(directoryPosition)
      filename = filesToRead(directoryPosition) //D-200001_merge_occ
      time = filename.split('/').last.stripPrefix("D-").stripSuffix("_merge_occ").toLong * 1000000000L
      cnt = time + 1
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
    }
    else {
      println("All files read "+ LocalDateTime.now())
    }
  }

  def fileToArray(pos:Int) ={
    println(s"Now reading ${filesToRead(pos)}")
    if(dropHeader){
      val br = new BufferedReader(new FileReader(filesToRead(pos)))
      br.readLine()
      br
    }
    else
      new BufferedReader(new FileReader(filesToRead(pos)))
  }

  def getListOfFiles(dir: String):Array[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f=> f.isFile && !f.isHidden).map(f=> f.getCanonicalPath).sorted
    } else {
      Array[String]()
    }
  }
}
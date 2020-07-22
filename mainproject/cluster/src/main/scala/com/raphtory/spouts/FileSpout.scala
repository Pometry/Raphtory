package com.raphtory.spouts

import java.io.File
import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.io
import scala.concurrent.duration.{Duration, NANOSECONDS}
import scala.io.Source

class FileSpout extends SpoutTrait {
  println("Start: " + LocalDateTime.now())
  val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean

  var filePosition         = 0
  var directoryPosition    = 0

  val filesToRead = if(fileName.isEmpty)
    getListOfFiles(directory)
  else
    Array(directory + "/" + fileName)

  var currentFile = fileToArray(directoryPosition)


  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
    case "nextLineBLock" => nextLineBlock()
    case "nextFile" => nextFile()
    case _ => println("message not recognized!")
  }

  def nextLineBlock() = {
    try {
      for (i <- 1 to 50) {
        sendTuple(currentFile(filePosition))
        filePosition += 1
      }
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
    }
    catch {
      case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
    }
  }

  def nextFile() = {
    directoryPosition += 1
    if (filesToRead.length > directoryPosition) {
      currentFile = fileToArray(directoryPosition)
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
    }
    else {
      println("All files read")
    }
  }


  def fileToArray(pos:Int) ={
    println(s"Now reading ${filesToRead(pos)}")
    if(dropHeader)
      Source.fromFile(filesToRead(pos)).getLines.drop(1).toArray
    else
      Source.fromFile(filesToRead(pos)).getLines.toArray
  }

  def getListOfFiles(dir: String):Array[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f=> f.isFile && !f.isHidden).map(f=> f.getCanonicalPath)
    } else {
      Array[String]()
    }
  }

}


package com.raphtory.spouts

import java.io.{BufferedReader, File, FileReader}
import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS, SECONDS}
import scala.io.Source

class FirehoseSpout extends SpoutTrait {

  println("Start: " + LocalDateTime.now())
  val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean
  var JUMP = System.getenv().getOrDefault("FILE_SPOUT_BLOCK_SIZE", "100").trim.toInt

  var directoryPosition    = 0

  val filesToRead = if(fileName.isEmpty)
    getListOfFiles(directory)
  else
    Array(directory + "/" + fileName)

  var currentFile = fileToArray(directoryPosition)


  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => {
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineBLock")
      AllocateSpoutTask(Duration(60, SECONDS), "increase")
    }
    case "increase" => JUMP += 2 ;AllocateSpoutTask(Duration(60, SECONDS), "increase")
    case "nextLineBLock" => nextLineBlock()
    case "nextFile" => nextFile()
    case _ => println("message not recognized!")
  }

  def nextLineBlock() = {
    try {
      for (i <- 1 to JUMP) {
        val line = currentFile.readLine()
        if(line!=null)
          sendTuple(line)
        else
          throw new Exception
      }
      AllocateSpoutTask(Duration(1, MILLISECONDS), "nextLineBLock")
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


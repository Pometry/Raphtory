package com.raphtory.examples.wordSemantic.spouts


import com.raphtory.spouts.{EtherFileReader, FileSpout}

import scala.concurrent.duration._
import scala.language.postfixOps

class CooccurrenceMatrixSpout extends EtherFileReader {
  var cnt = 1
  val JUMP2 = 20
  var posSlice = 1
  var cline = currentFile.readLine()
//  if (cline != null) {
  var currentLine = cline.split("\t")



  //  var currentLine = Array("")}
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
        for (i<- 1 to Set(JUMP, currentLine.length/JUMP2).min+1) {
          val currentSlice = currentLine.slice(posSlice, Set(posSlice + JUMP2, currentLine.length).min)
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

  override def nextLineBlock() = {
    //val filename = filesToRead(directoryPosition) //D-200001_merge_occ
   // var time = filename.split('/').last.stripPrefix("D-").stripSuffix("_merge_occ")
    try {
      //filePosition += 1
      cnt += 1
      cline = currentFile.readLine()
      currentLine = cline.split("\t")
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineSlice")
      }
    catch {
      case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
    }
  }
//    override def nextLineBlock() = {
//      try {
//        var del = 1000
//        val cline = currentFile.readLine()
//        if(cline!=null) {
//          val line = cline.split("\t")
//          val head = line(0)
//          for (i <- 1 until line.length - 1 by JUMP2) {
//            sendTuple(cnt.toString + ' ' + head + "\t" + line.slice(i, i + JUMP2).mkString("\t"))
//          }
//          cnt += 1
//          //del = 15 * line.length
//        }
//
//        AllocateSpoutTask(Duration(del, NANOSECONDS), "nextLineBLock")
//      }
//      catch {
//        case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
//      }
//    }
}
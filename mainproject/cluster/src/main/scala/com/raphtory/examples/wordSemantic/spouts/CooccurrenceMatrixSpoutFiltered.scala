package com.raphtory.examples.wordSemantic.spouts

import java.io.{BufferedReader, File, FileReader}
import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration._
import scala.language.postfixOps

class CooccurrenceMatrixSpoutFiltered extends CooccurrenceMatrixSpout {
 var freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
  var scale = scalling(freq)

  override def nextLineSlice() = {
    try {
      if (posSlice <= currentLine.length-1) {
        val head = currentLine(0)
        for (i<- 1 to Set(JUMP, currentLine.length-posSlice/JUMP2).min) {
          val currentSlice = currentLine.slice(posSlice, posSlice + JUMP2)
          sendTuple(cnt.toString + ' ' + scale.toString + ' ' + head + "\t" + currentSlice.mkString("\t"))
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
    try {
      cnt += 1
      cline = currentFile.readLine()
      currentLine = cline.split("\t")
      freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
      scale = scalling(freq)
      AllocateSpoutTask(Duration(1, NANOSECONDS), "nextLineSlice")
      }
    catch {
      case e:Exception => AllocateSpoutTask(Duration(1, NANOSECONDS), "nextFile")
    }
  }

  def scalling(freq: Array[Int]): Double = {
   math.sqrt(freq.map(math.pow(_, 2)).sum)
  }
}
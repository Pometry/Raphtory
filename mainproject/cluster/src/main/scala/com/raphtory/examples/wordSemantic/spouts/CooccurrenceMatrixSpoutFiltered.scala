package com.raphtory.examples.wordSemantic.spouts

import com.raphtory.core.model.communication.StringSpoutGoing
import com.raphtory.examples.wordSemantic.spouts.CooccurrenceMatrixSpout.Message.{NextFile,NextLineBlock, NextLineSlice}

import scala.language.postfixOps

class CooccurrenceMatrixSpoutFiltered extends CooccurrenceMatrixSpout {
 var freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
  var scale = scaling()

  override def nextLineSlice() = {
    try {
      if (posSlice <= currentLine.length-1) {
        val head = currentLine(0)
        for (i<- 1 to Set(JUMP, currentLine.length-posSlice/JUMP2).min) {
          val currentSlice = currentLine.slice(posSlice, posSlice + JUMP2)
          sendTuple(StringSpoutGoing(cnt.toString + ' ' + scale.toString + ' ' + head + "\t" + currentSlice.mkString("\t")))
          posSlice += JUMP2
        }
        self ! NextLineSlice //AllocateSpoutTask(Duration(1, MILLISECONDS), NextLineSlice)
      }
      else {
        posSlice = 1
        self ! NextLineBlock //AllocateSpoutTask(Duration(1, NANOSECONDS), NextLineBlock)
      }
    }catch {
      case e: Exception => println(e,  posSlice)
    }
  }

  override def nextLineBlock() = {
    try {
      cline = currentFile.readLine()
      currentLine = cline.split("\t")
      freq = currentLine.drop(2).grouped(2).map(_.head.toInt).toArray
      scale = scaling()
      cnt += 1
      self ! NextLineSlice //AllocateSpoutTask(Duration(1, NANOSECONDS), nextLineSlice)
      }
    catch {
      case e:Exception => self ! NextFile//AllocateSpoutTask(Duration(1, NANOSECONDS), nextFile)
    }
  }

  def scaling(): Double = {
   math.sqrt(freq.map(math.pow(_, 2)).sum)
  }
}
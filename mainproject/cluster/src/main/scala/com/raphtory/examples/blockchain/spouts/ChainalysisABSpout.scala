package com.raphtory.examples.blockchain.spouts

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class ChainalysisABSpout extends SpoutTrait {

  val file_name = System.getenv().getOrDefault("CHAINALYSIS_FILENAME", "/home/tsunade/qmul/datasets/chainalysis/abmshort.csv").trim
  val fileLines = Source.fromFile(file_name).getLines.drop(1).toArray
  // upstream/master
  var position    = 0
  var linesNumber = fileLines.length
  println("Starting File ingestion: " + LocalDateTime.now())
  println("Lines :" + linesNumber)

  override def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
    case "newLine" =>
      if (position < linesNumber) {
        sendTuple(fileLines(position))
        position += 1
        AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
      }
    case _ => println("message not recognized!")
  }
}
package com.raphtory.examples.blockchain.spouts

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class ChainalysisABSpout extends SpoutTrait {

  val file_name = System.getenv().getOrDefault("DATA_FILENAME", "/home/tsunade/qmul/datasets/chainalysis/abmsmall.csv").trim
  val fl = Source.fromFile(file_name)
  var linesNumber = Source.fromFile(file_name).getLines().size - 1
    val fileLines = fl.getLines.drop(1)//.toArray
  // upstream/master
  var position = 0
  val JUMP = 6
  println("Starting File ingestion: " + LocalDateTime.now())
  println("Lines :" + linesNumber)

  override def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
    case "newLine" =>

      if (position < linesNumber) {
        try {
          val l = fileLines.take(JUMP).mkString("\n")
          sendTuple(l)
          AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
        }catch{
          case e: NoSuchElementException => println("End of file!")
        }
        position += JUMP
      }
    case _ => println("message not recognized!")
  }
}
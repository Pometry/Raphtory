package com.raphtory.examples.gab.actors

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration._
import scala.io
import scala.io.Source
import scala.language.postfixOps

class GabExampleSpout extends SpoutTrait {

  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/app").trim
  val file_name = System.getenv().getOrDefault("GAB_FILE_NAME", "gabNetwork500.csv").trim
  val fileLines = Source.fromFile(directory + "/" + file_name).getLines.drop(1).toArray
  // upstream/master
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())


  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
    case "newLine" =>
      try {
        if (position < linesNumber) {
          for (i <- 1 to 100) {
            var line = fileLines(position)
            sendTuple(line)
            position += 1
          }
          AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
        }
        else {
          println("Finished ingestion")
          System.gc()
        }
      }catch {case e:Exception => println("Finished ingestion")}
    case _ => println("message not recognized!")
  }
}

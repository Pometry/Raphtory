package com.raphtory.examples.trackAndTrace.spouts

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class TrackAndTraceSpout extends SpoutTrait {

  val file_name = System.getenv().getOrDefault("TRACK_AND_TRACE_FILENAME", "/Users/mirate/Documents/phd/locationdataexample.csv").trim
  val fileLines = io.Source.fromFile(file_name).getLines.drop(3).toArray
  // upstream/master
  var position    = 0
  var linesNumber = fileLines.length
  println("Starting File ingestion: " + LocalDateTime.now())
  println("Lines :" + linesNumber)

  protected def ProcessSpoutTask(message: Any): Unit = message match {
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
package com.raphtory.examples.tsvnet

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

/** Spout for network datasets of the form SRC_NODE_ID DEST_NODE_ID TIMESTAMP */
class TSVSpout extends SpoutTrait {

  val directory = System.getenv().getOrDefault("TSV_DIRECTORY", "/app").trim
  val file_name = System.getenv().getOrDefault("TSV_FILE_NAME", "sx_reordered.txt").trim
  val fileLines = io.Source.fromFile(directory + "/" + file_name).getLines.drop(1).toArray
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
        }
      }catch {case e:Exception => println("Finished ingestion")}
    case _ => println("message not recognized!")
  }
}

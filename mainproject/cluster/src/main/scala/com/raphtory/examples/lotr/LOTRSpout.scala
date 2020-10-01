package com.raphtory.examples.lotr

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}
import scala.io

class LOTRSpout extends SpoutTrait {

  // Relating to where the file is
  val directory = System.getenv().getOrDefault("LOTR_DIRECTORY", "/Users/naomiarnold/CODE/Raphtory/LOTR").trim
  val file_name = System.getenv().getOrDefault("LOTR_FILE_NAME", "lotr.csv").trim
  val fileLines = io.Source.fromFile(directory + "/" + file_name).getLines.toArray

  // Initialise ready to read
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, MILLISECONDS), "newLine")
    case "newLine" =>
      if (position < linesNumber) {
        var line = fileLines(position)
        sendTuple(line)
        position += 1
        AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
      }
    case _ => println("message not recognized!")
  }
}

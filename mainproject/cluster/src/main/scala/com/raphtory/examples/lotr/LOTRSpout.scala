package com.raphtory.examples.lotr

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.components.Spout.SpoutTrait.BasicDomain
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.{Next}
import com.raphtory.core.model.communication.StringSpoutGoing

import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}
import scala.io

class LOTRSpout extends SpoutTrait[BasicDomain,StringSpoutGoing] {

  // Relating to where the file is
  val directory = System.getenv().getOrDefault("LOTR_DIRECTORY", "com/raphtory/example/lotr").trim
  val file_name = System.getenv().getOrDefault("LOTR_FILE_NAME", "lotr.csv").trim
  val fileLines = scala.io.Source.fromFile(directory + "/" + file_name).getLines.toArray

  // Initialise ready to read
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())

  override def handleDomainMessage(message: BasicDomain): Unit = message match {
    case Next =>
      if (position < linesNumber) {
        var line = fileLines(position)
        sendTuple(StringSpoutGoing(line))
        position += 1
        self ! Next
      }
    case _ => println("message not recognized!")
  }
  override def startSpout(): Unit = self ! Next
}

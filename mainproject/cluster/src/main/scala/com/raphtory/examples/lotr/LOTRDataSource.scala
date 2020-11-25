package com.raphtory.examples.lotr

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.{DataSource, DataSourceComplete, Spout}
import com.raphtory.core.model.communication.{SpoutGoing, StringSpoutGoing}

import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}
import scala.io

class LOTRDataSource extends DataSource {

  // Relating to where the file is
  val directory = System.getenv().getOrDefault("LOTR_DIRECTORY", "com/raphtory/example/lotr").trim
  val file_name = System.getenv().getOrDefault("LOTR_FILE_NAME", "lotr.csv").trim
  val fileLines = scala.io.Source.fromFile(directory + "/" + file_name).getLines.toArray

  // Initialise ready to read
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())

  override def setupDataSource(): Unit = {}

  override def generateData(): SpoutGoing = {
    if(position==linesNumber)
      throw new DataSourceComplete()
    else {
      val line =fileLines(position)
      position+=1
      StringSpoutGoing(line)
    }
  }

  override def closeDataSource(): Unit = {}
}

package com.raphtory.examples.lotr

import com.raphtory.core.components.Spout.{DataSource, DataSourceComplete}
import com.raphtory.core.model.communication.{SpoutGoing, StringSpoutGoing}

import scala.collection.mutable


class LOTRDataSource extends DataSource {

  val directory = System.getenv().getOrDefault("LOTR_DIRECTORY", "com/raphtory/example/lotr").trim
  val file_name = System.getenv().getOrDefault("LOTR_FILE_NAME", "lotr.csv").trim
  val fileQueue = mutable.Queue[String]() ++= scala.io.Source.fromFile(directory + "/" + file_name).getLines.toArray

  override def setupDataSource(): Unit = {}//no setup

  override def generateData(): SpoutGoing = {
    if(fileQueue isEmpty)
      throw new DataSourceComplete()
    else
      StringSpoutGoing(fileQueue.dequeue())
  }

  override def closeDataSource(): Unit = {}//no file closure already done
}

package com.raphtory.examples.lotr

import com.raphtory.core.components.Spout.{DataSource}
import com.raphtory.core.model.communication.{SpoutGoing, StringSpoutGoing}

import scala.collection.mutable


class LOTRDataSource extends DataSource[String] {

  val directory = System.getenv().getOrDefault("LOTR_DIRECTORY", "com/raphtory/example/lotr").trim
  val file_name = System.getenv().getOrDefault("LOTR_FILE_NAME", "lotr.csv").trim
  val fileQueue = mutable.Queue[String]() ++= scala.io.Source.fromFile(directory + "/" + file_name).getLines

  override def setupDataSource(): Unit = {}//no setup

  override def generateData(): Option[String] = {
    if(fileQueue isEmpty){
      dataSourceComplete()
      None
    }
    else
      Some(fileQueue.dequeue())
  }

  override def closeDataSource(): Unit = {}//no file closure already done
}

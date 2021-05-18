package com.raphtory.dev.lotr

import com.raphtory.core.actors.Spout.Spout
import scala.collection.mutable

class lotrSpout extends Spout[String] {
 val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
    val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //
    val filename = directory + "/" + fileName
  val fileQueue = mutable.Queue[String]()
	
  override def setupDataSource(): Unit = {
    fileQueue++=
      scala.io.Source.fromFile(filename)
        .getLines
  }//no setup

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

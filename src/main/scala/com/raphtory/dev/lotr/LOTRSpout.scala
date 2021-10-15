package com.raphtory.dev.lotr

import com.raphtory.core.components.spout.Spout
import scala.collection.mutable

class LOTRSpout(filename:String) extends Spout[String] {

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

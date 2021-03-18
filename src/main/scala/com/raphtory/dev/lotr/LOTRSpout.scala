package examples.lotr

import com.raphtory.core.actors.Spout.Spout

import scala.collection.mutable


class LOTRSpout extends Spout[String] {

  val fileQueue = mutable.Queue[String]()

  override def setupDataSource(): Unit = {
    fileQueue++=
      scala.io.Source.fromFile("src/main/scala/examples/lotr/lotr.csv")
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

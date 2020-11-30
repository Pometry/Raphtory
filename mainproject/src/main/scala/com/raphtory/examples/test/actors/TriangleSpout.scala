package com.raphtory.examples.test.actors

import com.raphtory.core.actors.Spout.Spout

import scala.collection.mutable.Queue

class TriangleSpout extends Spout[String] {

  val edges = Queue[String]("3,3,1", "4,3,4", "2,2,3", "5,4,1", "6,1,3", "1,1,2")

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[String] = {
    if(edges.isEmpty) {
      dataSourceComplete()
      None
    }
    else
      Some(edges.dequeue())
  }

  override def closeDataSource(): Unit = {}
}

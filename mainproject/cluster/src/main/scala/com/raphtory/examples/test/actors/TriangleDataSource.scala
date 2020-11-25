package com.raphtory.examples.test.actors

import com.raphtory.core.components.Spout.{DataSource}
import com.raphtory.core.model.communication.{SpoutGoing, StringSpoutGoing}

import scala.collection.mutable.Queue

class TriangleDataSource extends DataSource {

  val edges = Queue[StringSpoutGoing](StringSpoutGoing("3,3,1"), StringSpoutGoing("4,3,4"), StringSpoutGoing("2,2,3"), StringSpoutGoing("5,4,1"), StringSpoutGoing("6,1,3"), StringSpoutGoing("1,1,2"))

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[SpoutGoing] = {
    if(edges.isEmpty) {
      dataSourceComplete()
      None
    }
    else
      Some(edges.dequeue())
  }

  override def closeDataSource(): Unit = {}
}

package com.raphtory.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(msgID: Int, initialValue: Boolean, srcId: Int, dstId: Int)
    extends Entity(msgID, initialValue) {

  override def printProperties: String =
    s"Edge between $srcId and $dstId: with properties: " + System
      .lineSeparator() +
      super.printProperties()

  def killList(vKills: List[Int]): Unit = {
    vKills match {
      case head :: tail => {
        kill(head)
        killList(tail)
      }
      case _ =>
    }
  }
}

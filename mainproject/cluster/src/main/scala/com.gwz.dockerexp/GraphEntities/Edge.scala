package com.gwz.dockerexp.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(msgID: Int, initialValue: Boolean, srcId: Int, dstId: Int)
    extends Entity(msgID, initialValue) {

  override def printProperties: String =
    s"Edge between $srcId and $dstId: with properties: " + System
      .lineSeparator() +
      super.printProperties()

  def deprecateProperties(vKills: List[Int]): Unit = {
    vKills match {
      case head :: tail => {
        defineAbsentAtMoment(head)
        deprecateProperties(tail)
      }
      case _ =>
    }
  }
}

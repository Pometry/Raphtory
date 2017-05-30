package com.gwz.dockerexp.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(msgID:Int,initialValue:Boolean,src: Int, dst:Int) extends Entity(msgID,initialValue){
  val srcID = src
  val dstID = dst
  override def printProperties(): String = s"Edge between $srcID and $dstID: with properties: \n"+ super.printProperties()

  def killList(vKills:List[Int]):Unit={
    if(vKills!=Nil){ //while there are items in the list
      kill(vKills.head) //add the kill to the edge
      killList(vKills.tail) //recall the function with the rest of the list
    }
  }

}

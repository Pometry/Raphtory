package com.raphtory.examples.gabMining

import com.raphtory.core.components.Router.TraditionalRouter.Helpers.RouterSlave
import com.raphtory.core.model.communication._
import java.text.SimpleDateFormat

class GabMiningRouter (routerId:Int,override val initialManagerCount:Int) extends RouterSlave {
  println("INSIDE ROUTER")

  def parseRecord(record: Any): Unit = {
    println("INSIDE ROUTER DOS")
    val fileLine = record.asInstanceOf[String].split(";").map(_.trim)
    println("INSIDE ROUTER TRES "+ fileLine(0))
    val sourceNode=fileLine(2).toInt
    val targetNode=fileLine(5).toInt
    //val test=fileLine(0).slice(1,19).toString
    //println("DATE "+test)
    val creationDate = dateToUnixTime(timestamp=fileLine(0).slice(0,19))
    //val creationDate = dateToUnixTime(timestamp=fileLine(0))
    //val creationDate = 656566656L
    //2016-08-10T03:58:37+00:00

    //create sourceNode
    toPartitionManager(VertexAdd(routerId, creationDate, sourceNode))
    //create destinationNode
    toPartitionManager(VertexAdd(routerId, creationDate, targetNode))
    //create edge
    toPartitionManager(EdgeAdd(routerId, creationDate, sourceNode, targetNode))



  }

  def dateToUnixTime(timestamp: => String): Long = {
    //if(timestamp == null) return null;
    println("TIME FUNC: "+ timestamp)
    //val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+'HH:mm")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    //println(sdf)
    val dt = sdf.parse(timestamp)
    //println(dt)
    val epoch = dt.getTime()
    println(epoch)
    epoch / 1000

  }
}

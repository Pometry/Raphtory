package com.raphtory.dev.networkx

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

class networkxGraphBuilder extends GraphBuilder[String]{

  override def parseTuple(record: String) = {
      val dp = record.split(";").map(_.trim)
      val properties = dp(2).replaceAll("^\\{|\\}$", "").split(",")
        .map(_.split(":"))
        .map { case Array(key, value) => (key.trim()-> value.trim()) }.toMap
      val srcClusterId = dp(0).toLong
      val dstClusterId = dp(1).toLong
      val time = properties("'t'").toLong


    addVertex(time, srcClusterId, Properties(StringProperty("Word", dp.head)))
    addVertex(time, dstClusterId, Properties(StringProperty("Word", dp(1))))
    addEdge(time, srcClusterId, dstClusterId, Properties(DoubleProperty("weight", properties.getOrElse("'weight'", "1").toDouble)))



    //    sendUpdate(
//        EdgeAddWithProperties(msgTime = time,
//          srcID = dstClusterId,
//          dstID = srcClusterId ,
//          Properties(LongProperty("weight", properties("'weight'").toLong))
//        )
//      )

    }
}

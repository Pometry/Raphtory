package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.objectgraph.messaging._
import com.raphtory.core.model.graph.{FloatProperty, LongProperty, Properties, StringProperty}

class incCSVGB extends GraphBuilder[String] {

  override def parseTuple(row: String) =
    try {
      val dp            = row.split(",").map(_.trim)
      val occurenceTime = dp.head.toLong
      val srcClusterId  = assignID(dp(1))
      val dstClusterId  = assignID(dp(2))
      if (dp.length>3){
      val coocWeight    = dp(3).toFloat
        addVertex(updateTime = occurenceTime, srcId = srcClusterId, Properties(StringProperty("Word", dp(1))))
        addVertex(updateTime = occurenceTime, srcId = dstClusterId, Properties(StringProperty("Word", dp(2))))
        addEdge(
          updateTime = occurenceTime,
          srcId = srcClusterId,
          dstId = dstClusterId,
          Properties(FloatProperty("Frequency", coocWeight))
        )
      }else {
        addVertex(
                updateTime = occurenceTime,
                srcId = srcClusterId,
                Properties(
                        StringProperty("Word", dp(1)),
                        LongProperty("commLabel", dp(2).toLong)
                )
        )
      }
    } catch {
      case e: Exception => println(e, row)
    }
}

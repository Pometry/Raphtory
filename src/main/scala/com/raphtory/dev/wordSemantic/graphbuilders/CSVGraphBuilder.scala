package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

class CSVGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(row: String) =
    try {
      val dp = row.split(",").map(_.trim)
      val occurenceTime = dp.head.toLong
      val srcClusterId = assignID(dp(1))
      val dstClusterId = assignID(dp(2))
      val coocWeight = dp.last.toLong

      addVertex(updateTime = occurenceTime, srcId = srcClusterId, Properties(StringProperty("Word", dp(1))))
      addVertex(updateTime = occurenceTime, srcId = dstClusterId, Properties(StringProperty("Word", dp(2))))
      addEdge(
            updateTime = occurenceTime,
            srcId = srcClusterId,
            dstId = dstClusterId,
            Properties(DoubleProperty("Frequency", coocWeight))
          )

    } catch {
      case e: Exception => println(e, row)
    }
}

package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.{FloatProperty, Properties, StringProperty}

class CSVGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(row: String) = {
    //    try {
    val dp = row.split(",").map(_.trim)
    val occurenceTime = dp.head.toLong
    val srcClusterId = assignID(dp(1))
    val dstClusterId = assignID(dp(2))
    val coocWeight = dp.last.toFloat

    addVertex(updateTime = occurenceTime, srcId = srcClusterId, Properties(StringProperty("Word", dp(1))))
    addVertex(updateTime = occurenceTime, srcId = dstClusterId, Properties(StringProperty("Word", dp(2))))
    addEdge(
      updateTime = occurenceTime,
      srcId = srcClusterId,
      dstId = dstClusterId,
      Properties(FloatProperty("Frequency", coocWeight))
    )

  }
}

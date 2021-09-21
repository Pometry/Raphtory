package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.objectgraph.messaging._
import com.raphtory.core.model.graph.{FloatProperty, Properties, StringProperty}

class CoMatGB extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
    //println(record)
    try {
    var dp = tuple.split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)

      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      val len = dp.length

      addVertex(occurenceTime, srcClusterId, Properties(StringProperty("Word", dp.head)))

      for (i <- 1 until len by 2) {
        val dstClusterId = assignID(dp(i))
        val coocWeight = dp(i + 1).toLong

        addVertex(occurenceTime, dstClusterId, Properties(StringProperty("Word", dp(i))))
        addEdge(
          occurenceTime,
          srcClusterId,
          dstClusterId,
          Properties(FloatProperty("Frequency", coocWeight))
        )
      }

    }catch {
      case e: Exception => println(e, tuple)
    }
  }
}

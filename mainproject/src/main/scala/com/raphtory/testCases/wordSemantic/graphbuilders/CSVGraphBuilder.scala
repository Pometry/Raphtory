package com.raphtory.testCases.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._
import org.apache.spark.sql.Row

class CSVGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(row: String) =
    try {
      val dp = row.split(",").map(_.trim)
      val occurenceTime = dp.head.toLong
      val srcClusterId = assignID(dp(1))
      val dstClusterId = assignID(dp(2))
      val coocWeight = dp.last.toLong

      sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = srcClusterId, Properties(StringProperty("Word", dp(1)))))
      sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = dstClusterId, Properties(StringProperty("Word", dp(2)))))
      sendUpdate(
          EdgeAddWithProperties(
            msgTime = occurenceTime,
            srcID = srcClusterId,
            dstID = dstClusterId,
            Properties(LongProperty("Frequency", coocWeight))
          )
        )
    } catch {
      case e: Exception => println(e, row)
    }
}

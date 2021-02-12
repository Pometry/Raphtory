package com.raphtory.testCases.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

class CooccurrenceMatrixGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
    //println(record)
    var dp = tuple.split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    try {
      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      val len = dp.length

      sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcId = srcClusterId, Properties(StringProperty("Word", dp.head))))

      for (i <- 1 until len by 2) {
        val dstClusterId = assignID(dp(i))
        val coocWeight = dp(i + 1).toLong

        sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcId = dstClusterId, Properties(StringProperty("Word", dp(i)))))
        sendUpdate(
         EdgeAddWithProperties(
            msgTime = occurenceTime,
            srcId = srcClusterId,
            dstId = dstClusterId,
            Properties(LongProperty("Frequency", coocWeight))
          )
        )
      }

    }catch {
      case e: Exception => println(e, dp.length, tuple)
    }
  }
}

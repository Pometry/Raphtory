package com.raphtory.examples.wordSemantic.graphbuilders

import com.raphtory.core.components.Router.GraphBuilder
import com.raphtory.core.model.communication._

class CooccurrenceMatrixGraphBuilderFiltered extends GraphBuilder[String] {
  val THR = System.getenv().getOrDefault("COOC_FREQ_THRESHOLD ", "0.05").trim.toDouble

  override def parseTuple(tuple: String) = {
    //println(record)
    var dp =tuple.split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    val scale = dp(1).toDouble
    try {
      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      val len = dp.length
      for (i <- 1 until len by 2) {
        if ((dp(i+1).toLong/scale) >= THR) {
          val dstClusterId = assignID(dp(i))
          val coocWeight = dp(i + 1).toLong

          sendUpdate(
            EdgeAddWithProperties(
              msgTime = occurenceTime,
              srcID = srcClusterId,
              dstID = dstClusterId,
              Properties(LongProperty("Frequency", coocWeight),
                  DoubleProperty("ScaledFreq", coocWeight/scale))
            )
          )
        }
      }
    }catch {
      case e: Exception => println(e, dp.length, tuple.asInstanceOf[String])
    }
  }
}

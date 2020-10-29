package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._

class CooccurrenceMatrixRouterFiltered(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {
  val THR = System.getenv().getOrDefault("COOC_FREQ_THRESHOLD ", "0.05").trim.toDouble

  def parseTuple(record: Any): Unit = {
    //println(record)
    var dp = record.asInstanceOf[String].split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    val scale = dp(1).toDouble
    try {
      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      sendGraphUpdate(
        VertexAddWithProperties(
          msgTime = occurenceTime,
          srcID = srcClusterId,
          Properties(StringProperty("Word", dp.head))
        )
      )
      val len = dp.length
      for (i <- 1 until len by 2) {
        if ((dp(i+1).toLong/scale) >= THR) {
          val dstClusterId = assignID(dp(i))
          val coocWeight = dp(i + 1).toLong

          sendGraphUpdate(
            VertexAddWithProperties(
              msgTime = occurenceTime,
              srcID = dstClusterId,
              Properties(StringProperty("Word", dp(i)))
            )
          )

          sendGraphUpdate(
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
      case e: Exception => println(e, dp.length, record.asInstanceOf[String])
    }
  }
}

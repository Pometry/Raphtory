package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{Type, _}

class CooccurrenceMatrixRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val dp = record.asInstanceOf[String].split(" ").map(_.trim)
    val occurenceTime = dp(0).toLong
    val srcClusterId = assignID(dp(1))
    val len = dp.length

    for (i <- 2 until len by 2) {
      val dstClusterId = assignID(dp(i))
      val cooc_weight = dp(i+1).toLong

      sendGraphUpdate(
        EdgeAddWithProperties(msgTime = occurenceTime,
          srcID = srcClusterId,
          dstID = dstClusterId,
          Properties(DoubleProperty("Frequency", cooc_weight))
        )
      )
    }
  }
}

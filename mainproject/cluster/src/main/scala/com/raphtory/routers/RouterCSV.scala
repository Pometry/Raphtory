package com.raphtory.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._

class RouterCSV(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    try {
      val dp = record.asInstanceOf[String].split(",").map(_.trim)
      val time = dp.head.toLong
      val srcId = assignID(dp(1))
      val dstId = assignID(dp(2))

        sendGraphUpdate(
           EdgeAdd(
            msgTime = time,
            srcID = srcId,
            dstID = dstId
          )
        )
    }catch {
      case e: Exception => println(e, record.asInstanceOf[String])
    }
  }
}
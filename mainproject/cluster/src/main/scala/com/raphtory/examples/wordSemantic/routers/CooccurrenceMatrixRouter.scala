package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{Type, _}

class CooccurrenceMatrixRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    //println(record)
    var dp = record.asInstanceOf[String].split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    try {
      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      val len = dp.length
      for (i <- 1 until len by 2) {
        val dstClusterId = assignID(dp(i))
        val coocWeight = dp(i + 1).toLong

        sendGraphUpdate(
         EdgeAddWithProperties(
            msgTime = occurenceTime,
            srcID = srcClusterId,
            dstID = dstClusterId,
            Properties(DoubleProperty("Frequency", coocWeight))
          )
        )
      }
    }catch {
      case e: Exception => println(e, dp.length, record.asInstanceOf[String])
    }
  }

  def DateFormatting(date: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMM")
    format.parse(date).getTime
  }
}

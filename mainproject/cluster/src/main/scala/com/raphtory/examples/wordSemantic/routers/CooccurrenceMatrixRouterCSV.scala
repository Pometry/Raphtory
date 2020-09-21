package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._

class CooccurrenceMatrixRouterCSV(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    //println(record)
    try {
      val dp = record.asInstanceOf[String].split(",").map(_.trim)
      val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
      val srcClusterId = assignID(dp(1))
      val dstClusterId = assignID(dp(2))
    //  val coocWeight = dp(3).toLong

        sendGraphUpdate(
//          EdgeAddWithProperties(
           EdgeAdd(
            msgTime = occurenceTime,
            srcID = srcClusterId,
            dstID = dstClusterId//,
//            Properties(LongProperty("Frequency", coocWeight))
          )
        )
    }catch {
      case e: Exception => println(e, record.asInstanceOf[String])
    }
  }

//  def DateFormatting(date: String): Long = {
//    val format = new java.text.SimpleDateFormat("yyyyMM")
//    format.parse(date).getTime
//  }
}

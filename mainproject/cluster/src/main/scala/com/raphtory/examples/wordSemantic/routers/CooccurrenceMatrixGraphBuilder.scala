package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.{GraphBuilder, RouterWorker}
import com.raphtory.core.model.communication.{Type, _}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class CooccurrenceMatrixGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
    //println(record)
    var dp = tuple.split(" ").map(_.trim)
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    try {
      dp = dp.last.split("\t")
      val srcClusterId = dp.head.toLong//assignID(dp.head)
      val len = dp.length

      for (i <- 1 until len by 2) {
        val dstClusterId = dp(i).toLong//assignID(dp(i))
        val coocWeight = dp(i + 1).toLong

        sendUpdate(
         EdgeAddWithProperties(
            msgTime = occurenceTime,
            srcID = srcClusterId,
            dstID = dstClusterId,
            Properties(LongProperty("Frequency", coocWeight))
          )
        )
      }

    }catch {
      case e: Exception => println(e, dp.length, tuple)
    }
  }

  def DateFormatting(date: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMM")
    format.parse(date).getTime
  }
}

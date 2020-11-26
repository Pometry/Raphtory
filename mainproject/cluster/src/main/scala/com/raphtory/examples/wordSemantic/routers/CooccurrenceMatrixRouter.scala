package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{Type, _}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class CooccurrenceMatrixRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[String](routerId,workerID, initialManagerCount, initialRouterCount) {

  override protected def parseTuple(tuple: String): ParHashSet[GraphUpdate] = {
    //println(record)
    var dp = tuple.split(" ").map(_.trim)
    val commands = new ParHashSet[GraphUpdate]()
    val occurenceTime = dp.head.toLong//DateFormatting(dp.head) //.slice(4, dp.head.length)
    try {
      dp = dp.last.split("\t")
      val srcClusterId = dp.head.toLong//assignID(dp.head)
      val len = dp.length

      for (i <- 1 until len by 2) {
        val dstClusterId = dp(i).toLong//assignID(dp(i))
        val coocWeight = dp(i + 1).toLong

        commands+=(
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
    commands
  }

  def DateFormatting(date: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMM")
    format.parse(date).getTime
  }
}

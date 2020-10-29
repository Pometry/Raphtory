package com.raphtory.examples.wordSemantic.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{Type, _}

import scala.collection.mutable.ListBuffer

class CooccurrenceMatrixRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount) {

  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {
    //println(record)
    var dp = tuple.value.split(" ").map(_.trim)
    val commands = new ListBuffer[GraphUpdate]()
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
      case e: Exception => println(e, dp.length, tuple.value)
    }
    commands.toList
  }

  def DateFormatting(date: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMM")
    format.parse(date).getTime
  }
}

package com.raphtory.testCases.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

import scala.collection.mutable

class CoMatGBFiltered extends GraphBuilder[String] {

  val THR: Double = System.getenv().getOrDefault("COOC_FREQ_THRESHOLD ", "0.1").trim.toDouble

  override def parseTuple(tuple: String) = {
    //println(record)
    try {
      var dp = tuple.split(" ").map(_.trim)
      var occurenceTime = dp.head.toLong
      dp = dp.last.split("\t")
      val srcClusterId = assignID(dp.head)
      val dst = mutable.HashMap[String,Long]()
      dp.tail.grouped(2).foreach(x=> dst.put(x.head, x.last.toLong))
      val top = (dst.size * THR).toInt
      val thr = if (top > 0) dst.values.toArray.sorted.reverse.take(top).last else dst.values.max

      sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = srcClusterId, Properties(StringProperty("Word", dp.head))))
      dst.filter(_._2>=thr).foreach{ edge =>
        val dstClusterId = assignID(edge._1)
        val coocWeight = edge._2

        sendUpdate(VertexAddWithProperties(msgTime = occurenceTime, srcID = dstClusterId, Properties(StringProperty("Word", edge._1))))
        sendUpdate(
         EdgeAddWithProperties(
            msgTime = occurenceTime,
            srcID = srcClusterId,
            dstID = dstClusterId,
            Properties(LongProperty("Frequency", coocWeight))
          )
        )
        occurenceTime+=1
      }

    }catch {
      case e: Exception => println(e, tuple)
    }
  }
}

package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.{FloatProperty, Properties, StringProperty}

import scala.collection.mutable

class CoMatGBFiltered(THR:Double) extends GraphBuilder[String] {

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

      addVertex(updateTime = occurenceTime, srcId = srcClusterId, Properties(StringProperty("Word", dp.head)))
      dst.filter(_._2>=thr).foreach{ edge =>
        val dstClusterId = assignID(edge._1)
        val coocWeight = edge._2

        addVertex(updateTime = occurenceTime, srcId = dstClusterId, Properties(StringProperty("Word", edge._1)))
        addEdge(
            updateTime = occurenceTime,
            srcId = srcClusterId,
            dstId = dstClusterId,
            Properties(FloatProperty("Frequency", coocWeight))
          )

        occurenceTime+=1
      }

    }catch {
      case e: Exception => println(e, tuple)
    }
  }
}

package com.raphtory.dev.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._
import com.raphtory.dev.wordSemantic.spouts.Update


class CoMatParquetGB extends GraphBuilder[Update] {

  override def parseTuple(row: Update) =
    try {
      val time  = row._1
      val src   = row._2
      val dst   = row._3
      val srcID = assignID(src)
      val dstID = assignID(dst)
      val freq  = row._4

      addVertex(updateTime = time, srcId = srcID, Properties(StringProperty("Word", src)))
      addVertex(updateTime = time, srcId = dstID, Properties(StringProperty("Word", dst)))
      addEdge(
                      updateTime = time,
                      srcId = srcID,
                      dstId = dstID,
                      Properties(DoubleProperty("Frequency", freq.toDouble))
              )
    } catch {
      case e: Exception => println(e, row)
    }
}
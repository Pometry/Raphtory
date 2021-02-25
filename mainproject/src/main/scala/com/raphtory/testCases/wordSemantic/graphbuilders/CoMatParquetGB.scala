package com.raphtory.testCases.wordSemantic.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._
import org.apache.spark.sql.Row

class CoMatParquetGB extends GraphBuilder[Row] {

  override def parseTuple(row: Row) =
    try {
      val time  = row.getAs[Long](0)
      val src   = row.getAs[String](1)
      val dst   = row.getAs[String](2)
      val srcID = assignID(src)
      val dstID = assignID(dst)
      val freq  = row.getAs[Long](3)

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

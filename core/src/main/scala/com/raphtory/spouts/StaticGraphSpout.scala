package com.raphtory.spouts

import com.raphtory.api.input.Spout

import scala.io.Source

case class StaticGraphSpout(fileDataPath: String) extends Spout[String] {

  private val source = Source.fromFile(fileDataPath)
  private val lines  = source.getLines()
  private var lineNo = 1
  private var count  = 0

  override def hasNext: Boolean = lines.hasNext

  override def next(): String = {
    val line = lines.next()
    val data = s"$line $lineNo"
    lineNo += 1
    count += 1
    if (count % 100_000 == 0)
      logger.debug(s"File spout sent $count messages.")
    data
  }

  override def close(): Unit = {
    logger.debug(s"Spout for '$fileDataPath' finished, edge count: ${lineNo - 1}")
    source.close()
  }

  override def spoutReschedules(): Boolean = false
}

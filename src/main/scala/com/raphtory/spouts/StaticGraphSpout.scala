package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.io.Source

case class StaticGraphSpout(fileDataPath: String) extends Spout[String] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val source = Source.fromFile(fileDataPath)
  val lines  = source.getLines()
  var lineNo = 1
  var count  = 0

  override def hasNext(): Boolean = lines.hasNext

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

  override def hasNextIterator(): Boolean = lines.hasNext

  override def nextIterator(): Iterator[String] =
    lines.map { line =>
      val data = s"$line $lineNo"
      lineNo += 1
      count += 1
      if (count % 100_000 == 0)
        logger.debug(s"File spout sent $count messages.")
      data
    }

  override def executeNextIterator(): Unit =
    for (line <- nextIterator())
      try graphBuilder.parseTuple(line)
}

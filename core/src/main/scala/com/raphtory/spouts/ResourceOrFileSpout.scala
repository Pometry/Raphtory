package com.raphtory.spouts

import com.raphtory.api.input.{Spout, SpoutInstance}

import scala.io.Source
import scala.util.Try

case class ResourceOrFileSpout(resource: String) extends Spout[String] {
  override def buildSpout(): SpoutInstance[String] = new ResourceOrFileSpoutSpoutInstance(resource)
}

class ResourceOrFileSpoutSpoutInstance(resource: String) extends SpoutInstance[String] {

  private val source = Try{Source.fromResource(resource)}.orElse(Try{Source.fromFile(resource)}).get
  private val lines  = source.getLines()

  override def hasNext: Boolean = lines.hasNext

  override def next(): String = lines.next()

  override def close(): Unit = source.close()

  override def spoutReschedules(): Boolean = false

  override def nextIterator(): Iterator[String] = lines

}

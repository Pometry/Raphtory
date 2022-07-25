package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

import scala.io.Source

case class ResourceSpout(resource: String) extends Spout[String] {
  override def buildSpout(): SpoutInstance[String] = new ResourceSpoutInstance(resource)
}

class ResourceSpoutInstance(resource: String) extends SpoutInstance[String] {

  private val source = Source.fromResource(resource)
  private val lines  = source.getLines()

  override def hasNext: Boolean = lines.hasNext

  override def next(): String = lines.next()

  override def close(): Unit = source.close()

  override def spoutReschedules(): Boolean = false

  override def nextIterator(): Iterator[String] = lines

}

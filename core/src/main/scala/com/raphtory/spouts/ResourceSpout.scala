package com.raphtory.spouts

import com.raphtory.components.spout.Spout

import scala.io.Source

case class ResourceSpout(resource: String) extends Spout[String] {

  val source = Source.fromResource(resource)
  val lines  = source.getLines()

  override def hasNext: Boolean = lines.hasNext

  override def next(): String = lines.next()

  override def close(): Unit = source.close()

  override def spoutReschedules(): Boolean = false

  override def nextIterator(): Iterator[String] = lines

}

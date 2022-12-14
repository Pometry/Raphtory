package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

import java.nio.file.Paths
import scala.io.Source
import scala.util.Try

case class ResourceOrFileSpout(resource: String) extends Spout[String] {
  override def buildSpout(): SpoutInstance[String] = new ResourceOrFileSpoutSpoutInstance(resource)
}

class ResourceOrFileSpoutSpoutInstance(resource: String) extends SpoutInstance[String] {

  //very likely this should be in the test directory
  private val source = Try(Source.fromResource(resource))
    .orElse(Try(Source.fromResource(resource.replaceFirst("/", ""))))
    .orElse(Try(Source.fromResource(Paths.get(System.getenv("RAPHTORY_DATA"), resource).toString)))
    .orElse(Try(Source.fromFile(resource)))
    .orElse(Try(Source.fromFile(Paths.get(System.getenv("RAPHTORY_DATA"), resource).toFile)))
    .get
  private val lines  = source.getLines()

  override def hasNext: Boolean = lines.hasNext

  override def next(): String = lines.next()

  override def close(): Unit = source.close()

  override def spoutReschedules(): Boolean = false

  override def nextIterator(): Iterator[String] = lines

}

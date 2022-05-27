package com.raphtory.output.sinks

import com.raphtory.output.Sink

abstract class AbstractStreamSink(entityDelimiter: String) extends Sink[String] {
  private var justClosedEntity = false

  def write(value: String): Unit

  final override def writeEntity(entity: String): Unit = {
    append(entity)
    closeEntity()
  }

  final override def append(partialEntity: String): Unit = {
    if (justClosedEntity) {
      write(entityDelimiter)
      justClosedEntity = false
    }
    write(partialEntity)
  }

  final override def closeEntity(): Unit =
    justClosedEntity = true
}

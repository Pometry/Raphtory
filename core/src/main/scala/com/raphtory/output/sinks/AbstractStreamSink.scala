package com.raphtory.output.sinks

import com.raphtory.output.Sink

abstract class AbstractStreamSink(entityDelimiter: String) extends Sink[String] {

  def write(value: String): Unit

  private var streamIsEmpty = true

  final override def writeEntity(entity: String): Unit =
    if (streamIsEmpty)
      write(entityDelimiter + entity)
    else {
      write(entity)
      streamIsEmpty = true
    }

  private var justClosedEntity = false

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

package com.raphtory.output.sinks

import com.raphtory.output.Sink

abstract class AbstractMessageSink extends Sink[String] {
  def sendAsync(message: String): Unit

  private var currentPartialEntity = ""

  final override def writeEntity(entity: String): Unit = sendAsync(entity)

  final override def append(partialEntity: String): Unit = currentPartialEntity += partialEntity

  final override def closeEntity(): Unit = {
    sendAsync(currentPartialEntity)
    currentPartialEntity = ""
  }
}

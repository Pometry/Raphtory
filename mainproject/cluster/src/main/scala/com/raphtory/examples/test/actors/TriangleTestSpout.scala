package com.raphtory.examples.test.actors

import com.raphtory.core.components.Spout.SpoutTrait

class TriangleTestSpout extends SpoutTrait  {
  override protected def ProcessSpoutTask(receivedMessage: Any) = receivedMessage match {
    case StartSpout =>
      sendTuple("1,1,2")
      sendTuple("1,2,3")
      sendTuple("3,3,1")

  }
}

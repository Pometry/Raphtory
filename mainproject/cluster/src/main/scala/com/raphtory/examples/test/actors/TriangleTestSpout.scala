package com.raphtory.examples.test.actors

import com.raphtory.core.components.Spout.SpoutTrait

class TriangleTestSpout extends SpoutTrait  {
  override protected def ProcessSpoutTask(receivedMessage: Any) = receivedMessage match {
    case StartSpout =>
      sendTuple("1,1,2")
      sendTuple("2,2,3")
      sendTuple("3,3,1")
      sendTuple("4,3,4")
      sendTuple("5,4,1")
      sendTuple("6,1,3")

  }
}

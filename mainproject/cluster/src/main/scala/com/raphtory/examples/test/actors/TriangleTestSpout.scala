package com.raphtory.examples.test.actors

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.components.Spout.SpoutTrait.BasicDomain
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.Next
import com.raphtory.core.model.communication.StringSpoutGoing

class TriangleTestSpout extends SpoutTrait[BasicDomain,StringSpoutGoing]  {
  override def handleDomainMessage(message: BasicDomain): Unit = message match {
    case Next =>
      sendTuple(StringSpoutGoing("1,1,2"))
      sendTuple(StringSpoutGoing("2,2,3"))
      sendTuple(StringSpoutGoing("3,3,1"))
      sendTuple(StringSpoutGoing("4,3,4"))
      sendTuple(StringSpoutGoing("5,4,1"))
      sendTuple(StringSpoutGoing("6,1,3"))

  }

  override def startSpout(): Unit = self ! Next
}

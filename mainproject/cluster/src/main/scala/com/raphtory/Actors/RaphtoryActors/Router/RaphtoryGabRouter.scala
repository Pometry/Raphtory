package com.raphtory.Actors.RaphtoryActors.Router

import com.raphtory.caseclass._
import com.raphtory.utils.Utils.getManager

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import spray.json._

/**
  * The Graph Manager is the top level actor in this system (under the stream)
  * which tracks all the graph partitions - passing commands processed by the 'command processor' actors
  * to the correct partition
  */

/**
  * The Command Processor takes string message from Kafka and translates them into
  * the correct case Class which can then be passed to the graph manager
  * which will then pass it to the graph partition dealing with the associated vertex
  */

final class RaphtoryGabRouter(override val routerId:Int, override val initialManagerCount:Int) extends RouterTrait {
  import com.raphtory.caseclass.RaphtoryJsonProtocol._

  override def parseJSON(command:String) : Unit= {
    super.parseJSON(command)
    val parsedOBJ: Command = command.parseJson.convertTo[Command]
    val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)
  }

  override protected def otherMessages(rcvdMessage: Any): Unit = {
    println(s"Message not recognized ${rcvdMessage.getClass}")
  }

}

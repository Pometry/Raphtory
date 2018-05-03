package com.raphtory.examples.gab.actors

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.router.RouterTrait
import com.raphtory.core.model.communication.Command
import com.raphtory.core.utils.Utils.getManager
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
  import com.raphtory.core.model.communication.RaphtoryJsonProtocol._
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

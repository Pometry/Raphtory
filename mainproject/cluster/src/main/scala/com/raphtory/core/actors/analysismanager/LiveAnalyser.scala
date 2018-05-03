package com.raphtory.core.actors.analysismanager

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication.{LiveAnalysis, PartitionsCount, Results}
import com.raphtory.core.utils.Utils
import com.raphtory.core.analysis.{Analyser, TestAnalyser}

trait LiveAnalyser extends RaphtoryActor {
  private var managerCount : Int = 0
  protected val mediator = DistributedPubSub(context.system).mediator

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  override def receive: Receive = {
    case "analyse" => analyse()
    case Results(result) => this.processResults(result)
    case PartitionsCount(newValue) => {
      managerCount = newValue
      println(s"Maybe a new PartitionManager has arrived: ${newValue}")
    }
  }

  private final def analyse() ={
      for(i <- 0 until managerCount){
        mediator ! DistributedPubSubMediator.Send(s"/user/Manager_$i",
          LiveAnalysis(
            Class.forName(sys.env.getOrElse("ANALYSISTASK", classOf[TestAnalyser].getClass.getName))
              .newInstance().asInstanceOf[Analyser]),
          false)
      }
  }

  protected def processResults(result : Any)

}

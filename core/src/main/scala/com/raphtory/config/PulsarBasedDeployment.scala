package com.raphtory.config

object PulsarBasedDeployment extends PulsarConnector {

  def apply(): (Scheduler, Gateway) = {
    val scheduler: Scheduler = new MonixScheduler()
    val gateway: Gateway     = new SingleConnectorGateway(new PulsarConnector())

    (scheduler, gateway)
  }
}

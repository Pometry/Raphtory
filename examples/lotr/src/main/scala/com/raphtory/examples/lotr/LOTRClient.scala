package com.raphtory.examples.lotr

import com.raphtory.algorithms.generic.BlergoBlargo
import com.raphtory.Raphtory
import com.raphtory.sinks.{FileSink, PrintSink}

object LOTRClient extends App {

  val customConfig: Map[String, String] = Map(
          ("raphtory.pulsar.admin.address", "http://127.0.0.1:8080"),
          ("raphtory.pulsar.broker.address", "pulsar://127.0.0.1:6650"),
          ("raphtory.zookeeper.address", "127.0.0.1:2181")
  )

  val client = Raphtory.connect(customConfig)

//  val output = FileSink("/tmp/raphtory")

  val progressTracker = client.execute(new BlergoBlargo()).writeTo(PrintSink())

  progressTracker.waitForJob()

  client.disconnect()

}

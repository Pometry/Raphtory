package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.Raphtory
import com.raphtory.sinks.FileSink

object LOTRClient extends App {

  val client = Raphtory.connect()

  val output = FileSink("/tmp/raphtory")

  client.execute(ConnectedComponents()).writeTo(output)

}

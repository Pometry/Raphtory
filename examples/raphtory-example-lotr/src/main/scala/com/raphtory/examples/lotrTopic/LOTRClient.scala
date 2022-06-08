package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat

object LOTRClient extends App {

  val client = Raphtory.connect()

val output  = FileOutputFormat("/tmp/raphtory")

  client.execute(ConnectedComponents()).writeTo(output)
  
}

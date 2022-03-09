package com.raphtory.ethereum

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.deploy.Raphtory
import com.raphtory.output.FileOutputFormat

object Client extends App {

  val client = Raphtory.createClient()
  client.pointQuery(
          ConnectedComponents(),
          FileOutputFormat("/tmp/ethereum/ConnectedComponents"),
          1591951621
  )

}

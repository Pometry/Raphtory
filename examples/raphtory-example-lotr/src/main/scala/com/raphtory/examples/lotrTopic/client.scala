package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.config.ConfigHandler
import com.raphtory.deployment.Raphtory
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.output.FileOutputFormat

object client extends App {

    val customConfig:  Map[String, Any] = Map("raphtory.deploy.id" -> "raphtory_167133814", "raphtory.deploy.distributed" -> true)
    val client = Raphtory.createClient(customConfig = customConfig)
    val output       = FileOutputFormat("/tmp/raphtoryds2")
    val queryHandler = client.pointQuery(DegreesSeparation(), output, 32674)
    queryHandler.waitForJob()

}

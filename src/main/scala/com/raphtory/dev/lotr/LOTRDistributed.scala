package com.raphtory.dev.lotr

import com.raphtory.core.build.server.RaphtoryService
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.spouts.FileSpout

class LOTRDistributed extends RaphtoryService[String]{

  override def defineSpout(): Spout[String] =  new FileSpout("src/main/scala/com/raphtory/dev/lotr/")

  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()

}


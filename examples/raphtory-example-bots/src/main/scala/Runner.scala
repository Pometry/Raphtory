import analysis.{MemberRank, TemporalMemberRank}
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "/tmp/000.json"

  val source  = FileSpout(path)
  val builder = new BotsFromJsonGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    .at(1643336173)
    .past()
    .execute(EdgeList())
    .writeTo(output)

  queryHandler.waitForJob()

}

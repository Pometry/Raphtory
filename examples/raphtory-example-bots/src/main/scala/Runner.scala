import analysis.{MemberRank, TemporalMemberRank}
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import graphbuilders.{BotsFromJsonGraphBuilder, BotsGraphBuilder}

import scala.language.postfixOps

object Runner extends App {
  val path = "/Pometry/cleanedData5000/000.json"

  val source  = FileSpout(path)
  val builder = new BotsFromJsonGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    .range(1543277951, 1543414175, 500000000)
    //.at(1543277951)
    .past()
    .execute(ConnectedComponents())
//    .transform(PageRank())
//    .execute(MemberRank() -> TemporalMemberRank())
    .writeTo(output)

  queryHandler.waitForJob()

}

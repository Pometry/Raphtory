package com.raphtory.examples.twitter.higgsdataset

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList, NodeList}
import com.raphtory.algorithms.generic.centrality.{Degree, PageRank, WeightedDegree}
import com.raphtory.algorithms.generic.motif.ThreeNodeMotifs
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{Graph, GraphBuilder, ImmutableString, Properties, Source, Type}
import com.raphtory.examples.twitter.higgsdataset.analysis.MemberRank
import com.raphtory.examples.twitter.higgsdataset.analysis.TemporalMemberRank
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

/**
  * This Runner runs PageRank, chained with MemberRank and TemporalMemberRank which are two custom written algorithms
  * to find potential bot activity in the Higgs Twitter data.
  */

object LotrGraphBuilder extends GraphBuilder[String] {

  override def apply(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableString("name", sourceNode)), Type("Character"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableString("name", targetNode)), Type("Character"))
    graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurrence"))
  }
}

object HiggsRunner extends RaphtoryApp.Local {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val spout  = FileSpout(path)
      val source = Source(spout, LotrGraphBuilder)
      val output = FileSink("/tmp/higgsoutput")

      graph.load(source)
      //get simple metrics
      graph.at(1341705593).past().transform(ThreeNodeMotifs).execute(NodeList()).writeTo(output).waitForJob()
      //execute(NodeList())
//      graph
//        .execute(Degree())
//        .writeTo(output)
//        .waitForJob()
//
//      //Chained Algorithm Example
//      graph
//        .at(1341705593)
//        .past()
//        .transform(PageRank())
//        .transform(MemberRank())
//        .execute(TemporalMemberRank())
//        .writeTo(output)
//        .waitForJob()
    }
}

package com.raphtory.examples.coho.companiesStream.algorithms

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

import scala.collection.View

/**
  * Algorithm to obtain PSCs of companies and all the people they control companies with, creates edges between PSCs.
  * Also creates edges between companies and PSCs. Edge weights show percentage of share ownership that PSC has in the company
  * and the date that the PSC was appointed on.
  */

class CompanyDirectorGraph extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.Type == "Company") {
          val edges                     = vertex.inEdges
          val directorIDs: View[String] = for (edge <- edges) yield edge.getPropertyOrElse("psc", "")
          vertex.messageInNeighbours((vertex.name, directorIDs.toList))
        }
      }
      .step { vertex =>
        if (vertex.Type == "Persons With Significant Control") {
          val timeandcompany = vertex.outEdges.flatMap { edge =>
            val companyNumber  = edge.getPropertyOrElse("companyNumber", "")
            val shareOwnership = edge.getPropertyOrElse("shareOwnership", 0)

            edge.history().map { e =>
              s"${e.time}-$companyNumber-$shareOwnership"
            }
          }

          val messageQueue = vertex.messageQueue[(String, List[String])]
          val companies    = messageQueue.map(_._1)
          val peeps        = messageQueue.flatMap(_._2)

          val listofpsc = peeps
            .groupBy(x => x)
            .map {
              case (key, vals) => (key, vals.length)
            }
            .-(vertex.name)

          if (listofpsc.nonEmpty) {
            vertex.setState("list of pscs", listofpsc)
            vertex.setState("companies", companies)
            vertex.setState("time and company", timeandcompany)
          }
        }
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.select("name", "timeAndCompany", "listOfPSCs", "companies")

}

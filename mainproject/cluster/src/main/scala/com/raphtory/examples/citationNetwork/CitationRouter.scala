package com.raphtory.examples.citationNetwork

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class CitationRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount, initialRouterCount) {
  override protected def parseTuple(tuple: StringSpoutGoing): ParHashSet[GraphUpdate] = {
    val fileLine = tuple.value.split(",").map(_.trim) //take the tuple and split on , as we are only interested in the first 4 fields
    // title_paper,year,volume,title,pages,number,journal,author,ENTRYTYPE,ID
    val sourceTitle = fileLine(0)
    val destinationTitle = fileLine(3)
    val destinationYear = fileLine(1).toLong
    val sourceYear = 2020 //toBeCreated

    val sourceID = assignID(sourceTitle)
    val destinationID = assignID(destinationTitle)
    val commands = new ParHashSet[GraphUpdate]()
    //create sourceNode
    commands+=(VertexAddWithProperties(
      sourceYear, //when it happened ??
      sourceID, // the id of the node
      Properties(ImmutableProperty("title", sourceTitle)), //properties for the node
      Type("Publication")) //node type
    )
    //create destinationNode
    commands+=(VertexAddWithProperties(
      destinationYear,
      destinationID,
      Properties(ImmutableProperty("title", destinationTitle)),
      Type("Publication"))
    )

    //create edge
    commands+=(EdgeAdd(
      sourceYear, //time of edge ??
      sourceID, //source of edge
      destinationID, //destination of edge
      Type("Cited") // edge type
    ))

    commands
  }
}

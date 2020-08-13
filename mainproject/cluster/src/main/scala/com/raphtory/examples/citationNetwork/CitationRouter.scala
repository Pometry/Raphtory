package com.raphtory.examples.citationNetwork

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._

class CitationRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val fileLine = record.asInstanceOf[String].split(",").map(_.trim) //take the tuple and split on , as we are only interested in the first 4 fields
    // title_paper,year,volume,title,pages,number,journal,author,ENTRYTYPE,ID
    val sourceTitle = fileLine(0)
    val destinationTitle = fileLine(3)
    val destinationYear = fileLine(1).toLong
    val sourceYear = 2020 //toBeCreated

    val sourceID = assignID(sourceTitle)
    val destinationID = assignID(destinationTitle)

    //create sourceNode
    sendGraphUpdate(VertexAddWithProperties(
      sourceYear, //when it happened ??
      sourceID, // the id of the node
      Properties(ImmutableProperty("title", sourceTitle)), //properties for the node
      Type("Publication")) //node type
    )
    //create destinationNode
    sendGraphUpdate(VertexAddWithProperties(
      destinationYear,
      destinationID,
      Properties(ImmutableProperty("title", destinationTitle)),
      Type("Publication"))
    )

    //create edge
    sendGraphUpdate(EdgeAdd(
      sourceYear, //time of edge ??
      sourceID, //source of edge
      destinationID, //destination of edge
      Type("Cited") // edge type
    ))


  }
}

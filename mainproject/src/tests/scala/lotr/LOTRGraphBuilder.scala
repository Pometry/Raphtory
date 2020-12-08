package lotr

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

class LOTRGraphBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple: String) = {

    val fileLine = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    sendUpdate(VertexAddWithProperties(timeStamp, srcID,
      Properties(ImmutableProperty("name",sourceNode)),Type("Character")))
    sendUpdate(VertexAddWithProperties(timeStamp, tarID,
      Properties(ImmutableProperty("name",targetNode)),Type("Character")))

    sendUpdate(EdgeAdd(timeStamp,srcID,tarID, Type("Character Co-occurence")))
  }
}

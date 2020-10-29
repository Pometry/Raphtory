package com.raphtory.examples.ldbc.routers

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, EdgeDelete, GraphUpdate, StringSpoutGoing, Type, VertexAdd, VertexDelete}
import com.raphtory.examples.test.actors.RandomSpout

import scala.collection.mutable.ListBuffer

class LDBCRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount) {
  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {
    val commands = new ListBuffer[GraphUpdate]()
    val fileLine           = tuple.asInstanceOf[String].split("\\|")
    val date               = fileLine(1).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
    val date2              = fileLine(2).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
    val creationDate: Long = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date).getTime()
    val deletionDate: Long = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date2).getTime()
    val vertexDeletion = sys.env.getOrElse("LDBC_VERTEX_DELETION", "false").trim.toBoolean
    val edgeDeletion = sys.env.getOrElse("LDBC_EDGE_DELETION", "false").trim.toBoolean
    fileLine(0) match {
      case "person" =>
        commands+=(VertexAdd(creationDate, assignID("person" + fileLine(3)), Type("person")))
        //sendGraphUpdate(VertexAdd(creationDate, fileLine(3).toLong,Type("person")))
        if(vertexDeletion)
          commands+=(VertexDelete(deletionDate, assignID("person" + fileLine(3))))
      case "person_knows_person" =>
        //sendGraphUpdate(EdgeAdd(creationDate, fileLine(3).toLong,fileLine(4).toLong,Type("person_knows_person")))
        commands+=(
                EdgeAdd(
                        creationDate,
                        assignID("person" + fileLine(3)),
                        assignID("person" + fileLine(4)),
                        Type("person_knows_person")
                )
        )
        if(edgeDeletion)
          commands+=(EdgeDelete(deletionDate, assignID("person"+fileLine(3)),assignID("person"+fileLine(4))))
    }
    commands.toList
  }
}
//2012-11-01T09:28:01.185+00:00|2019-07-22T11:24:24.362+00:00|35184372093644|Jose|Garcia|female|1988-05-20|111.68.47.44|Firefox

package com.raphtory.examples.ldbc.routers

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, EdgeDelete, GraphUpdate, StringSpoutGoing, Type, VertexAdd, VertexDelete}

import scala.collection.mutable.ListBuffer

class LDBCOldRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount, initialRouterCount) {
  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {

    val fileLine = tuple.asInstanceOf[String].split("\\|")
    val commands = new ListBuffer[GraphUpdate]()
    //val deletionDate:Long  = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date2).getTime()
    fileLine(0) match {
      case "person" =>
        val date = fileLine(6).substring(0, 10) + fileLine(5).substring(11, 23); //extract the day of the event
        //val date2 = fileLine(2).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
        val creationDate: Long = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date).getTime()
        commands+=(VertexAdd(creationDate, assignID("person" + fileLine(1)), Type("person")))
        commands.toList
      //sendGraphUpdate(VertexAdd(creationDate, fileLine(3).toLong,Type("person")))
      //    sendGraphUpdate(VertexDelete(deletionDate, assignID("person"+fileLine(3))))
      case "person_knows_person" =>
        val date = fileLine(3).substring(0, 10) + fileLine(3).substring(11, 23); //extract the day of the event
        //val date2 = fileLine(2).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
        val creationDate: Long = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date).getTime()
        //sendGraphUpdate(EdgeAdd(creationDate, fileLine(3).toLong,fileLine(4).toLong,Type("person_knows_person")))
        commands+=(
                EdgeAdd(
                        creationDate,
                        assignID("person" + fileLine(1)),
                        assignID("person" + fileLine(2)),
                        Type("person_knows_person")
                )
        )
        commands.toList
      //sendGraphUpdate(EdgeDelete(deletionDate, assignID("person"+fileLine(3)),assignID("person"+fileLine(4))))
    }
  }
}
//2012-11-01T09:28:01.185+00:00|2019-07-22T11:24:24.362+00:00|35184372093644|Jose|Garcia|female|1988-05-20|111.68.47.44|Firefox

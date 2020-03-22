package com.raphtory.examples.ldbc.routers

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, EdgeDelete, Type, VertexAdd, VertexDelete}

class LDBCRouter(routerId:Int, override val initialManagerCount:Int) extends RouterWorker{
  override protected def parseTuple(value: Any): Unit = {


    val fileLine = value.asInstanceOf[String].split("\\|")
    val date = fileLine(1).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
    val date2 = fileLine(2).substring(0, 10) + fileLine(1).substring(11, 23); //extract the day of the event
    val creationDate:Long  = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date).getTime()
    val deletionDate:Long  = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date2).getTime()
    fileLine(0) match {
      case "person" => {
        sendGraphUpdate(VertexAdd(creationDate, assignID("person"+fileLine(3)),Type("person")))
        //sendGraphUpdate(VertexAdd(creationDate, fileLine(3).toLong,Type("person")))
        sendGraphUpdate(VertexDelete(deletionDate, assignID("person"+fileLine(3))))
      }
      case "person_knows_person" => {
        //sendGraphUpdate(EdgeAdd(creationDate, fileLine(3).toLong,fileLine(4).toLong,Type("person_knows_person")))
        sendGraphUpdate(EdgeAdd(creationDate, assignID("person"+fileLine(3)),assignID("person"+fileLine(4)),Type("person_knows_person")))
        sendGraphUpdate(EdgeDelete(deletionDate, assignID("person"+fileLine(3)),assignID("person"+fileLine(4))))
      }
    }
  }
}

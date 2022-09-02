package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input._
import com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments.AppointmentListJsonProtocol.OfficerAppointmentListFormat
import com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments.OfficerAppointmentList
import spray.json._
import java.time.{LocalDate,LocalTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/**
 * Graph Builder for graph of officer to company, used data from Officer Appointment API
 */
class OfficerToCompanyGraphBuilder extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {
    try {
      val command = tuple
      val appointmentList = command.parseJson.convertTo[OfficerAppointmentList]
      sendAppointmentListToPartitions(appointmentList, graph)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    def sendAppointmentListToPartitions(
                                         appointmentList: OfficerAppointmentList, graph: Graph): Unit = {
      val officerId = appointmentList.links.get.self.get.split("/")(2)

      var tupleIndex = 1 //index * 50

      appointmentList.items.get.foreach { item =>
        if (item.appointed_on.nonEmpty && item.appointed_to.nonEmpty) {

         val name = item.name.get
          val companyNumber = item.appointed_to.get.company_number.get
          val resignedOnParsed =
            LocalDate.parse(item.resigned_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000
          val appointedOn =
            item.appointed_on.get
          val resignedOn =
            item.resigned_on.get
          val appointedOnParsed =
            LocalDate.parse(item.appointed_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000

          val difference = resignedOnParsed - appointedOnParsed

          graph.addVertex(
            appointedOnParsed,
            assignID(officerId),
            Properties(ImmutableProperty("name", officerId)),
            Type("Officer ID"),
            tupleIndex
          )

//          addVertex(
//            appointedOnParsed,
//            assignID(companyNumber),
//            Properties(ImmutableProperty("name", companyNumber)),
//            Type("Company Number"),
//            tupleIndex
//          )

          graph.addVertex(
            appointedOnParsed,
            difference,
            Properties(ImmutableProperty("name", difference.toString)),
            Type("Company Duration"),
            tupleIndex
          )

          graph.addEdge(
            appointedOnParsed,
            assignID(officerId),
            difference,
            Properties(LongProperty("weight", difference)),
            Type("Officer to Company Duration"),
            tupleIndex
          )

          tupleIndex += 1
        }
      }


    }

  }
}

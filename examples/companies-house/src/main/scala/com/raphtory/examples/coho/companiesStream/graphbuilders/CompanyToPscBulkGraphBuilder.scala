package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, Properties, Type}
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PersonWithSignificantControlStream
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PscStreamJsonProtocol.PersonWithSignificantControlStreamFormat
import spray.json._

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset}


class CompanyToPscBulkGraphBuilder extends GraphBuilder[String] {
  override def parseTuple(tuple: String): Unit = {
    try {
      val command = tuple
      val psc = command.parseJson.convertTo[PersonWithSignificantControlStream]
      sendPscToPartitions(psc)
    } catch {
      case e: Exception =>  e.printStackTrace()
    }
  }
    def sendPscToPartitions(psc: PersonWithSignificantControlStream) = {


        var tupleIndex = index * 50

        val notifiedOn =
          LocalDate.parse(psc.data.get.notified_on.getOrElse("1800-01-01").replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000

        val companyNumber = psc.company_number.get

        val pscId = psc.data.get.links.get.self.get.split("/")(5)

        addVertex(
          notifiedOn,
          assignID(pscId),
          Properties(ImmutableProperty("name", pscId)),
          Type("Persons With Significant Control"),
          tupleIndex
        )

        addVertex(
          notifiedOn,
          assignID(companyNumber),
          Properties(ImmutableProperty("name", companyNumber)),
          Type("Company"),
          tupleIndex
        )

        addEdge(
          notifiedOn,
          assignID(pscId),
          assignID(companyNumber),
          Properties(ImmutableProperty("company name", companyNumber)),
          Type("Psc to Company Duration"),
          tupleIndex
        )

        tupleIndex += 1
      }


}

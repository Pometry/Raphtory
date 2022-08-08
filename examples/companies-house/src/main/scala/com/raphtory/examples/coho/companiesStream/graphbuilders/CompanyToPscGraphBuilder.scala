package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, Properties, Type}
import spray.json._
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PersonWithSignificantControlItem
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PscItemJsonProtocol.ItemsFormat

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset}

class CompanyToPscGraphBuilder extends GraphBuilder[String] {
  override def parseTuple(tuple: String): Unit = {
    try {
      val psc = tuple.parseJson.convertTo[PersonWithSignificantControlItem]
      sendPscToPartitions(psc)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    def sendPscToPartitions(psc: PersonWithSignificantControlItem) = {

      var tupleIndex = index * 50
      val notifiedOn =
         LocalDate.parse(psc.notified_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000
      val pscId = psc.links.get.self.get.split("/")(5)
      val companyNumber = psc.links.get.self.get.split("/")(2)

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
}

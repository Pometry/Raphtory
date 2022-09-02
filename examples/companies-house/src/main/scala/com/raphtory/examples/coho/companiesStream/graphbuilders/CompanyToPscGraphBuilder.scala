package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, IntegerProperty, Properties, Type}
import spray.json._
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PersonWithSignificantControlItem
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PscItemJsonProtocol.ItemsFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset}

/**
 * Graph Builder mapping company to PSC for data obtained from Companies House API
 * This is the graph builder used to obtain vertices of company to psc with edges
 * labelled with share ownership and date notified on.
 */
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

      val companyNumber = psc.links.get.self.get.split("/")(2)

      val name = psc.name.getOrElse("No Name").split(" ")
      var dateOfBirth = "00"

        if (psc.date_of_birth.nonEmpty) {
          dateOfBirth = s"${psc.date_of_birth.get.month.get}-${psc.date_of_birth.get.year.get}"
        }

      val hyphenName = name.head match {
        case "\"Mr" | "\"Mr." | "\"Mrs" | "\"Mrs." | "\"Miss" | "\"Ms" | "\"Ms." | "\"M/S" | "\"Dr." | "\"Dr" | "\"Lord" => name.slice(1, name.length).mkString("-").replaceAll("\"", "")
        case _ => name.mkString("-").replaceAll("\"", "")
      }
      val nameID = s"$hyphenName-${dateOfBirth}"

      def matchControl(statement: Option[String]): Int = {
        statement.get match {
          case "ownership-of-shares-25-to-50-percent" |
               "ownership-of-shares-25-to-50-percent-as-trust" |
               "ownership-of-shares-25-to-50-percent-as-firm" |
               "ownership-of-shares-more-than-25-percent-registered-overseas-entity" |
               "ownership-of-shares-more-than-25-percent-as-trust-registered-overseas-entity" |
               "ownership-of-shares-more-than-25-percent-as-firm-registered-overseas-entity" => 25
          case "ownership-of-shares-50-to-75-percent" |
               "ownership-of-shares-50-to-75-percent-as-trust" |
               "ownership-of-shares-50-to-75-percent-as-firm" => 50
          case "ownership-of-shares-75-to-100-percent" |
               "ownership-of-shares-75-to-100-percent-as-trust" |
               "ownership-of-shares-75-to-100-percent-as-firm" =>  75
          case _ =>  0
        }
      }

      val naturesOfControl = psc.natures_of_control.getOrElse(List("None"))
      val shareOwnership = matchControl(naturesOfControl.headOption)
      if (notifiedOn > 0) {

          addVertex(
          notifiedOn,
          assignID(nameID),
          Properties(ImmutableProperty("name", nameID)),
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
          assignID(nameID),
          assignID(companyNumber),
          Properties(ImmutableProperty("psc", nameID),ImmutableProperty("companyNumber", companyNumber),IntegerProperty("shareOwnership", shareOwnership)),
          Type("Psc to Company"),
          tupleIndex
        )
      }
      tupleIndex += 1
    }

  }
}

package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, IntegerProperty, LongProperty, Properties, Type}
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PersonWithSignificantControlStream
import com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl.PscStreamJsonProtocol.PersonWithSignificantControlStreamFormat
import spray.json._
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset}


class CompanyToPscBulkGraphBuilder extends GraphBuilder[String] {
  override def parseTuple(tuple: String): Unit = {
    try {
      val psc = tuple.parseJson.convertTo[PersonWithSignificantControlStream]
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

      val name = psc.data.get.name.getOrElse("No Name").split(" ")

      var dateOfBirth = "00"
        if (psc.data.get.date_of_birth.nonEmpty) {
          dateOfBirth = s"${psc.data.get.date_of_birth.get.month.get}-${psc.data.get.date_of_birth.get.year.get}"
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

      val naturesOfControl = psc.data.get.natures_of_control.getOrElse(List("None"))
      val shareOwnership = matchControl(naturesOfControl.headOption)


        if (notifiedOn > 0) {
        // Edge for PSC to Company, weight of share ownership
//            addVertex(
//              notifiedOn,
//              assignID(nameID),
//              Properties(ImmutableProperty("name", nameID)),
//              Type("Persons With Significant Control"),
//              tupleIndex
//            )
//
//            addVertex(
//              notifiedOn,
//              assignID(companyNumber),
//              Properties(ImmutableProperty("name", companyNumber)),
//              Type("Company"),
//              tupleIndex
//            )

//            addEdge(
//              notifiedOn,
//              assignID(nameID),
//              assignID(companyNumber),
//              Properties(IntegerProperty("weight", shareOwnership)),
//              Type("Psc to Company Duration"),
//              tupleIndex
//            )

          if (psc.data.get.ceased_on.nonEmpty) {

            val ceasedOn = LocalDate.parse(psc.data.get.ceased_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000

            // Edge for PSC to Company that has been ceased, weight is shared ownership
            addVertex(
              ceasedOn,
              assignID(nameID),
              Properties(ImmutableProperty("name", nameID)),
              Type("Persons With Significant Control"),
              tupleIndex
            )

            addVertex(
              ceasedOn,
              assignID(companyNumber),
              Properties(ImmutableProperty("name", companyNumber)),
              Type("Company"),
              tupleIndex
            )

            addEdge(
              ceasedOn,
              assignID(nameID),
              assignID(companyNumber),
              Properties(IntegerProperty("weight", shareOwnership)),
              Type("Psc to Ceased Company Duration"),
              tupleIndex
            )
          }

        }

          tupleIndex += 1

      }


}

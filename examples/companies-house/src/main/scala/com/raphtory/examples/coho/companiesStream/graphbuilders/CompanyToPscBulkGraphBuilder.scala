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


      def matchControl(statement: String): Int = {
        statement match {
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

      val naturesOfControl: String = psc.data.get.natures_of_control.getOrElse(List("None")).head
      val shareOwnership = matchControl(naturesOfControl)

      if (psc.data.get.ceased_on.nonEmpty) {
        val ceasedOn = LocalDate.parse(psc.data.get.ceased_on.get.replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000

        if (notifiedOn > 0) {
          val companyDuration = ceasedOn - notifiedOn
          addVertex(
            companyDuration,
            assignID(nameID),
            Properties(ImmutableProperty("name", nameID)),
            Type("Persons With Significant Control")
          )

          addVertex(
            companyDuration,
            assignID(companyNumber),
            Properties(ImmutableProperty("name", companyNumber)),
            Type("Company")
          )

          addEdge(
            companyDuration,
            assignID(nameID),
            assignID(companyNumber),
            Properties(IntegerProperty("weight", shareOwnership)),
            Type("Psc to Company Duration")
          )

        }

      }

//        addVertex(
//          notifiedOn,
//          assignID(nameID),
//          Properties(ImmutableProperty("name", nameID)),
//          Type("Persons With Significant Control")
//        )


//      addVertex(
//          notifiedOn,
//          assignID(companyNumber),
//          Properties(ImmutableProperty("name", companyNumber)),
//          Type("Company")
//        )
//
//        addEdge(
//          notifiedOn,
//          assignID(nameID),
//          assignID(companyNumber),
//          Properties(IntegerProperty("weight", shareOwnership)),
//          Type("Psc to Company Duration")
//        )


      }


}

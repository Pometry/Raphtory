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
      val command = tuple
      val psc = command.parseJson.convertTo[PersonWithSignificantControlStream]
      sendPscToPartitions(psc)
    } catch {
      case e: Exception =>  e.printStackTrace()
    }
  }
    def sendPscToPartitions(psc: PersonWithSignificantControlStream) = {



        val notifiedOn =
          LocalDate.parse(psc.data.get.notified_on.getOrElse("1800-01-01").replaceAll("\"", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN) * 1000

        val companyNumber = psc.company_number.get

        val pscId = psc.data.get.links.get.self.get.split("/")(5)

     val naturesOfControl: String = psc.data.get.natures_of_control.get.head

      def matchControl(statement: String): Int = {
        statement match {
          case "ownership-of-shares-25-to-50-percent" | "ownership-of-shares-25-to-50-percent-as-trust" | "ownership-of-shares-25-to-50-percent-as-firm" => 25
          case "ownership-of-shares-50-to-75-percent" | "ownership-of-shares-50-to-75-percent-as-trust" | "ownership-of-shares-50-to-75-percent-as-firm" => 50
          case "ownership-of-shares-75-to-100-percent" | "ownership-of-shares-75-to-100-percent-as-trust" | "ownership-of-shares-75-to-100-percent-as-firm" =>  75
          case _ =>  0
        }
      }

      val shareOwnership = matchControl(naturesOfControl)

        addVertex(
          notifiedOn,
          assignID(pscId),
          Properties(ImmutableProperty("name", pscId)),
          Type("Persons With Significant Control")

        )

        addVertex(
          notifiedOn,
          assignID(companyNumber),
          Properties(ImmutableProperty("name", companyNumber)),
          Type("Company")

        )

        addEdge(
          notifiedOn,
          assignID(pscId),
          assignID(companyNumber),
          Properties(IntegerProperty("weight", shareOwnership)),
          Type("Psc to Company Duration")
        )


      }


}

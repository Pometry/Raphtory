package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, Properties, StringProperty, Type}
import com.raphtory.examples.coho.companiesStream.rawModel.CompaniesHouseJsonProtocol.{CompanyFormat, getField}
import com.raphtory.examples.coho.companiesStream.rawModel.Company
import spray.json._
import java.text.SimpleDateFormat
import java.util.Date

/**
 * The CompaniesStreamPersonGraphBuilder sets source node as the company
 * and the target node as the person of significant control, therefore the edge represents
 * companies linked to a specific person.
 */

class CompaniesStreamPersonGraphBuilder extends GraphBuilder[String] {
  private val nullStr = "null"
  override def parseTuple(tuple: String): Unit = {
    try {
      val command = tuple
      val company = command.parseJson.convertTo[Company]
      sendCompanyToPartitions(company)
    } catch {
      case e: Exception => e.printStackTrace
    }

    def getTimestamp(dateString: String): Long = {
      val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
      var date: Date = new Date()
      try {
        date = dateFormat.parse(dateString)
      } catch {
        case e: java.text.ParseException => 0
      }
     date.getTime
    }

    def sendCompanyToPartitions(
                                 company: Company
                               ): Unit = {
      val timeFromCoho = company.data.get.date_of_creation.get
      val timestamp = getTimestamp(timeFromCoho)

      for (data <- company.data) {
        val companyHash = data.company_number.getOrElse("0")
        val srcID = assignID(companyHash)
        addVertex(
          timestamp,
          srcID,
          Properties(ImmutableProperty("company_number", companyHash)),
          Type("Company")
        )

        for (links <- data.links) {
          val personHash = links.persons_with_significant_control.getOrElse(nullStr)
          val dstID = assignID(personHash)
          addVertex(
            timestamp,
            dstID,
            Properties(ImmutableProperty("person_sig_control", personHash)),
            Type("Person")
          )
          addEdge(timestamp, srcID, dstID, Properties(StringProperty("type", "Company to Person")))
        }
      }

    }
  }

}

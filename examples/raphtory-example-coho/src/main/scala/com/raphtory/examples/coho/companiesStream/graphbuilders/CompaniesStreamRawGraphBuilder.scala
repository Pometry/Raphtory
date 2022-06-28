package com.raphtory.examples.coho.companiesStream.graphbuilders

import com.raphtory.api.input.{BooleanProperty, GraphBuilder, IntegerProperty, Properties, StringProperty}
import com.raphtory.examples.coho.companiesStream.rawModel._
import spray.json._
import java.time.OffsetDateTime

class CompaniesStreamRawGraphBuilder extends GraphBuilder[String] {
  private val nullStr = "null"

  import com.raphtory.examples.coho.companiesStream.rawModel.CompaniesHouseJsonProtocol.CompanyFormat

  override def parseTuple(tuple: String) = {
    try {
      val command = tuple
      val company = command.parseJson.convertTo[Company]
      sendCompanyToPartitions(company)
    } catch {
      case e: Exception => e.printStackTrace
    }

    def sendCompanyToPartitions(
                               company: Company
                               ): Unit  = {
      val companyNumber = company.resource_id.get
      val timestamp = OffsetDateTime.parse(company.data.get.date_of_creation.get).toEpochSecond
        addVertex(
          timestamp,
          companyNumber,
          Properties(
            StringProperty(
              "resource_kind",
              company.resource_kind match {
                case Some(kind) => kind
                case None => nullStr
              }
            ),
            StringProperty(
              "resource_uri",
              company.resource_uri match {
                case Some(uri) => uri
                case None => nullStr
              }
            ),
            StringProperty(
              "resource_id",
              company.resource_id match {
                case Some(id) => id.toString
                case None => nullStr
              }
            ),
            StringProperty(
              "data",
              company.data match {
                case Some(data) => data.company_number.toString
                case None => nullStr
              }
            ),
            StringProperty(
              "event",
              company.event match {
                case Some(event) => event.timepoint.toString
                case None => nullStr
              }
            )
          )
        )

      // CompanyProfile resource data
        for (data <- company.data) {
          val dataCompanyNumber = data.company_number.get.toLong
          addVertex(
            timestamp,
            companyNumber,
            Properties(
              StringProperty("can_file", data.can_file.get.toString),
              StringProperty("company_name", data.company_name.get),
              StringProperty("company_number", data.company_number.get),
              StringProperty("company_status", data.company_status.get),
              StringProperty(
                "company_status_detail",
                data.company_status_detail match {
                  case Some(company_status_detail) => company_status_detail
                  case None => nullStr
                }
              ),
              StringProperty(
                "date_of_cessation",
                data.date_of_cessation match {
                  case Some(date_of_cessation) => date_of_cessation
                  case None => nullStr
                }
              ),
              StringProperty("date_of_creation", data.date_of_creation.get),
              StringProperty(
                "etag",
                data.etag match {
                  case Some(etag) => etag
                  case None => nullStr
                }
              ),
              BooleanProperty(
                "has_been_liquidated",
                data.has_been_liquidated match {
                  case Some(has_been_liquidated) => has_been_liquidated
                  case None => false
                }
              ),
              BooleanProperty(
                "has_charges",
                data.has_charges match {
                  case Some(has_charges) => has_charges
                  case None => false
                }
              ),
              BooleanProperty(
                "has_insolvency_history",
                data.has_insolvency_history match {
                  case Some(has_insolvency_history) => has_insolvency_history
                  case None => false
                }
              ),
              BooleanProperty(
                "is_community_interest_company",
                data.is_community_interest_company match {
                  case Some(is_community_interest_company) => is_community_interest_company
                  case None => false
                }
              ),
              StringProperty("jurisdiction", data.jurisdiction.get),
              StringProperty(
                "last_full_members_list_date",
                data.last_full_members_list_date match {
                  case Some(last_full_members_list_date) => last_full_members_list_date
                  case None => nullStr
                }
              ),
              BooleanProperty(
                "registered_office_is_in_dispute",
                data.registered_office_is_in_dispute match {
                  case Some(registered_office_is_in_dispute) => registered_office_is_in_dispute
                  case None => false
                }
              ),
              StringProperty(
                "sic_codes",
                data.sic_codes match {
                  case Some(sic_codes) => sic_codes
                  case None => nullStr
                }
              ),
              BooleanProperty(
                "undeliverable_registered_office_address",
                data.undeliverable_registered_office_address match {
                  case Some(undeliverable_registered_office_address) => undeliverable_registered_office_address
                  case None => false
                }
              ),
              StringProperty("type", data._type.get)
            )
          )
          //Edge from company to companyProfile resource data
          addEdge(timestamp, companyNumber, dataCompanyNumber, Properties(StringProperty("type", "companyToData")))

          //Company Accounts Information
          for (account <- data.accounts) {
            //The Accounting Reference Date (ARD) of the company
            for (accountingReference <- account.accounting_reference_date) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  IntegerProperty("accounting_reference_date_day", accountingReference.day.get),
                  IntegerProperty("accounting_reference_date_month", accountingReference.month.get)
                )
              )
            }

            //The last company accounts filed
            for (lastAccounts <- account.last_accounts) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  StringProperty("last_accounts_made_up_to", lastAccounts.made_up_to.get),
                  StringProperty("last_accounts_type", lastAccounts._type.get)
                )
              )
            }

            //Dates of next company accounts due/made up to + if company accounts overdue
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("next_due", account.next_due.get),
                StringProperty("next_made_up_to", account.next_made_up_to.get),
                BooleanProperty("overdue", account.overdue.get)
              )
            )
            //Edge from data to accounts
            val dst = account.accounting_reference_date.get.day.get.hashCode() + account.accounting_reference_date.get.month.get.hashCode()
            addEdge(timestamp, dataCompanyNumber, dst, Properties(StringProperty("type", "dataToAccountsInformation")))
          }


          //Annual return information
          for (annualReturn <- data.annual_return) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("last_made_up_to", annualReturn.last_made_up_to.get),
                StringProperty("next_due", annualReturn.next_due.get),
                StringProperty("next_made_up_to", annualReturn.next_made_up_to.get),
                BooleanProperty("overdue", annualReturn.overdue.get)
              )
            )
            val dst = annualReturn.next_due.hashCode()
            //Edge from data to annual return
            addEdge(timestamp, dataCompanyNumber, dst, Properties(StringProperty("type", "dataToAnnualReturn")))
          }

          //UK branch of a foreign company
          for (branchCompanyDetails <- data.branch_company_details) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("business_activity", branchCompanyDetails.business_activity.get),
                StringProperty("parent_company_name", branchCompanyDetails.parent_company_name.get),
                StringProperty("parent_company_number", branchCompanyDetails.parent_company_number.get)
              )
            )
            val dst = branchCompanyDetails.parent_company_number.get.hashCode()
            //Edge from data to branch company details
            addEdge(timestamp, dataCompanyNumber, dst, Properties(StringProperty("type", "dataToBranchCompanyDetails")))
          }

          //Confirmation Statement
          for (confirmationStatement <- data.confirmation_statement) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("last_made_up_to", confirmationStatement.last_made_up_to.get),
                StringProperty("next_due", confirmationStatement.next_due.get),
                StringProperty("next_made_up_to", confirmationStatement.next_made_up_to.get),
                BooleanProperty("overdue", confirmationStatement.overdue.get)
              )
            )
            val dst = confirmationStatement.next_due.get.hashCode()
            //Edge from data to confirmation statement information
            addEdge(timestamp, dataCompanyNumber, dst, Properties(StringProperty("type", "dataToConfirmationStatement")))
          }

          //Foreign Company Details
          for (foreignCompanyDetails <- data.foreign_company_details) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("business_activity", foreignCompanyDetails.business_activity.get),
                StringProperty("company_type", foreignCompanyDetails.company_type.get),
                StringProperty("governed_by", foreignCompanyDetails.governed_by.get),
                BooleanProperty("is_a_credit_finance_institution", foreignCompanyDetails.is_a_credit_finance_institution.get),
                StringProperty("registration_number", foreignCompanyDetails.registration_number.get)
              )
            )
            val dst = foreignCompanyDetails.registration_number.get.hashCode()
            //Edge from data to foreign company details
            addEdge(timestamp, dataCompanyNumber, dst, Properties(StringProperty("type", "dataToForeignCompanyDetails")))

            //Accounts Requirement
            for (accountingRequirement <- foreignCompanyDetails.accounting_requirement) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  StringProperty("foreign_account_type", accountingRequirement.foreign_account_type.get),
                  StringProperty("terms_of_account_publication", accountingRequirement.terms_of_account_publication.get),
                )
              )
              val src = foreignCompanyDetails.registration_number.get.hashCode()
              val dst = accountingRequirement.terms_of_account_publication.get.hashCode()
              //Edge from data to accounting requirement
              addEdge(timestamp, src, dst, Properties(StringProperty("type", "foreignCompanyDetailsToAccountingRequirement")))
            }

            //Foreign company account information
            for (accounts <- foreignCompanyDetails.accounts) {
              val accountDst = accounts.hashCode()
              addEdge(timestamp, foreignCompanyDetails.registration_number.get.hashCode(), accountDst, Properties(StringProperty("type", "foreignCompanyDetailsToAccounts")))
              //Date account period starts under parent law
              for (accountFrom <- accounts.account_period_from) {
                addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                    IntegerProperty("account_period_from_day", accountFrom.day.get),
                    IntegerProperty("account_period_from_month", accountFrom.month.get)
                  )
                )
                addEdge(timestamp, accountDst, accountFrom.day.get.hashCode(), Properties(StringProperty("type", "accountsToAccountPeriodFrom")))
              }

              //Date account period ends under parent law
              for (accountTo <- accounts.account_period_to) {
                addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                    IntegerProperty("account_period_to_day", accountTo.day.get),
                    IntegerProperty("account_period_to_month", accountTo.month.get)
                  )
                )
                addEdge(timestamp, accountDst, accountTo.day.get.hashCode(), Properties(StringProperty("type", "accountsToAccountPeriodTo")))
              }

              //Time allowed from period end for disclosure of accounts under parent law
              for(mustFileWithin <- accounts.must_file_within) {
                addVertex(
                  timestamp,
                  companyNumber,
                  Properties(
                    IntegerProperty("must_file_within_months", mustFileWithin.months.get)
                  )
                )
                addEdge(timestamp, accountDst, mustFileWithin.months.get.hashCode(), Properties(StringProperty("type", "accountsToMustFileWithin")))
              }
            }

            //Company origin informations
            for(originatingRegistry <- foreignCompanyDetails.originating_registry) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  StringProperty("originating_registry_name", originatingRegistry.name.get),
                  StringProperty("originating_registry_country", originatingRegistry.country.get)
                )
              )
              addEdge(timestamp, originatingRegistry.name.get.hashCode(), dst, Properties(StringProperty("type", "foreignCompanyDetailsToOriginatingRegistry")) )
            }
          }

          // A set of URLs related to the resource, including self
          for (links <- data.links) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("persons_with_significant_control", links.persons_with_significant_control.get),
                StringProperty("persons_with_significant_control_statement", links.persons_with_significant_control_statements.get),
                StringProperty("registers", links.registers.get),
                StringProperty("self", links.self.get)
              )
            )
            addEdge(timestamp, dataCompanyNumber, links.self.get.hashCode(), Properties(StringProperty("type", "dataToLinks")))
          }

          //The previous names of this company
          for (previousCompanyNames <- data.previous_company_names) {
            addVertex(
              timestamp,
              companyNumber,
              Properties(
                StringProperty("ceased_on", previousCompanyNames.ceased_on.get),
                StringProperty("effective_from", previousCompanyNames.effective_from.get),
                StringProperty("name", previousCompanyNames.name.get)
              )
            )
            addEdge(timestamp, dataCompanyNumber, previousCompanyNames.name.get.hashCode(), Properties(StringProperty("type", "dataToPreviousNames")))
          }
            //The address of the company's registered office
            for (registeredOfficeAddress <- data.registered_office_address) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  StringProperty("address_line_1", registeredOfficeAddress.address_line_1.get),
                  StringProperty("address_line_2", registeredOfficeAddress.address_line_2.get),
                  StringProperty("care_of", registeredOfficeAddress.care_of.get),
                  StringProperty("country", registeredOfficeAddress.country.get),
                  StringProperty("locality", registeredOfficeAddress.locality.get),
                  StringProperty("po_box", registeredOfficeAddress.po_box.get),
                  StringProperty("postal_code", registeredOfficeAddress.postal_code.get),
                  StringProperty("premises", registeredOfficeAddress.premises.get),
                  StringProperty("region", registeredOfficeAddress.region.get)
                )
              )
              addEdge(timestamp, dataCompanyNumber, registeredOfficeAddress.postal_code.get.hashCode, Properties(StringProperty("type", "dataToRegisteredAddress")))
            }

            for (serviceAddress <- data.service_address) {
              addVertex(
                timestamp,
                companyNumber,
                Properties(
                  StringProperty("address_line_1", serviceAddress.address_line_1.get),
                  StringProperty("address_line_2", serviceAddress.address_line_2.get),
                  StringProperty("care_of", serviceAddress.care_of.get),
                  StringProperty("country", serviceAddress.country.get),
                  StringProperty("locality", serviceAddress.locality.get),
                  StringProperty("po_box", serviceAddress.po_box.get),
                  StringProperty("postal_code", serviceAddress.postal_code.get),
                  StringProperty("region", serviceAddress.region.get)
                )
              )
              addEdge(timestamp, dataCompanyNumber, serviceAddress.postal_code.get.hashCode(), Properties(StringProperty("type", "dataToServiceAddress")))
            }


        }

      //Link to the related resource
      for (event <- company.event) {
        addVertex(
          timestamp,
          companyNumber,
          Properties(
            StringProperty("fields_changed", event.fields_changed.get.mkString),
            IntegerProperty("timepoint", event.timepoint.get),
            StringProperty("published_at", event.published_at.get),
            StringProperty("_type", event._type.get)
          )
        )
        addEdge(timestamp, companyNumber, event.published_at.get.hashCode(), Properties(StringProperty("type", "companyToEvent")))
      }
    }
  }
}

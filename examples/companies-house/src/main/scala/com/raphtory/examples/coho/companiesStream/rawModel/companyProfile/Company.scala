package com.raphtory.examples.coho.companiesStream.rawModel.companyProfile

/**
 * Case classes for all the Companies House fields in the Company Information Stream.
 */

case class Company(
                               resource_kind: Option[String],
                               resource_uri: Option[String],
                               resource_id: Option[String],
                               data: Option[Data],
                               event: Option[Event]
                             )

case class Data(
               accounts: Option[Accounts],
               annual_return: Option[AnnualReturn],
               branch_company_details: Option[BranchCompanyDetails],
               can_file: Option[Boolean],
               company_name: Option[String],
               company_number: Option[String],
               company_status: Option[String],
               company_status_detail: Option[String],
               confirmation_statement: Option[ConfirmationStatement],
               date_of_cessation: Option[String],
               date_of_creation: Option[String],
               etag: Option[String],
               foreign_company_details: Option[ForeignCompanyDetails],
               has_been_liquidated: Option[Boolean],
               has_charges: Option[Boolean],
               has_insolvency_history: Option[Boolean],
               is_community_interest_company: Option[Boolean],
               jurisdiction: Option[String],
               last_full_members_list_date: Option[String],
               links: Option[Links],
               previous_company_names: Option[Array[PreviousCompanyNames]],
               registered_office_address: Option[RegisteredOfficeAddress],
               registered_office_is_in_dispute: Option[Boolean],
               service_address: Option[ServiceAddress],
               sic_codes: Option[String],
               _type: Option[String],
               undeliverable_registered_office_address: Option[Boolean]
               )

case class Accounts(
                     account_period_from: Option[AccountPeriodFrom],
                     account_period_to: Option[AccountPeriodTo],
                     accounting_reference_date: Option[AccountingReferenceDate],
                     last_accounts: Option[LastAccounts],
                     must_file_within: Option[MustFileWithin],
                     next_accounts: Option[NextAccounts],
                     next_due: Option[String],
                     next_made_up_to: Option[String],
                     overdue: Option[Boolean]
                   )

case class AccountingReferenceDate(
                                  day: Option[String],
                                  month: Option[String]
                                  )

case class LastAccounts(
                       made_up_to: Option[String],
                       _type: Option[String]
                       )

case class NextAccounts(
                       due_on: Option[String],
                       period_end_on: Option[String],
                       period_start_on: Option[String]
                       )

case class ConfirmationStatement(
                                  last_made_up_to: Option[String],
                                  next_due: Option[String],
                                  next_made_up_to: Option[String],
                                  overdue: Option[Boolean]
                                )

case class Links(
                  persons_with_significant_control: Option[String],
                  persons_with_significant_control_statements: Option[String],
                  registers: Option[String],
                  self: Option[String]
                )

case class RegisteredOfficeAddress(
                                    address_line_1: Option[String],
                                    address_line_2: Option[String],
                                    care_of: Option[String],
                                    country: Option[String],
                                    locality: Option[String],
                                    po_box: Option[String],
                                    postal_code: Option[String],
                                    premises: Option[String],
                                    region: Option[String]
                                  )

case class Event(
                fields_changed: Option[Array[String]],
                timepoint: Option[Int],
                published_at: Option[String],
                _type: Option[String]
                )

case class AnnualReturn(
                       last_made_up_to: Option[String],
                       next_due: Option[String],
                       next_made_up_to: Option[String],
                       overdue: Option[Boolean]
                       )

case class BranchCompanyDetails(
                               business_activity: Option[String],
                               parent_company_name: Option[String],
                               parent_company_number: Option[String]
                               )

case class ForeignCompanyDetails(
                                accounts: Option[Accounts],
                                accounting_requirement: Option[AccountingRequirement],
                                business_activity: Option[String],
                                company_type: Option[String],
                                governed_by: Option[String],
                                is_a_credit_finance_institution: Option[Boolean],
                                originating_registry: Option[OriginatingRegistry],
                                registration_number: Option[String]
                                )

case class AccountingRequirement(
                                foreign_account_type: Option[String],
                                terms_of_account_publication: Option[String]
                                )

case class AccountPeriodFrom(
                                day: Option[String],
                                month: Option[String]
                            )

case class AccountPeriodTo(
                          day: Option[String],
                          month: Option[String]
                          )

case class MustFileWithin(
                         months: Option[String]
                         )

case class OriginatingRegistry(
                              country: Option[String],
                              name: Option[String]
                              )


case class PreviousCompanyNames(
                               ceased_on: Option[String],
                               effective_from: Option[String],
                               name: Option[String]
                               )

case class ServiceAddress(
                         address_line_1: Option[String],
                         address_line_2: Option[String],
                         care_of: Option[String],
                         country: Option[String],
                         locality: Option[String],
                         po_box: Option[String],
                         postal_code: Option[String],
                         region: Option[String]
                         )

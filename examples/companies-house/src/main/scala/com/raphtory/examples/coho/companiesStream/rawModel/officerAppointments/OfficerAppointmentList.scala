package com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments

case class OfficerAppointmentList(
                                   date_of_birth: Option[DateOfBirth],
                                   etag: Option[String],
                                   is_corporate_officer: Option[Boolean],
                                   items: Option[List[Items]],
                                   items_per_page: Option[String],
                                   kind: Option[String],
                                   links: Option[Links],
                                   name: Option[String],
                                   start_index: Option[String],
                                   total_results: Option[String]
                                 )

case class DateOfBirth(
                        month: Option[Int],
                        year: Option[Int]
                      )

case class Items(
                  address: Option[Address],
                  appointed_before: Option[String],
                  appointed_on: Option[String],
                  appointed_to: Option[AppointedTo],
                  contact_details: Option[ContactDetails],
                  country_of_residence: Option[String],
                  former_names: Option[List[FormerNames]],
                  identification: Option[Identification],
                  is_pre_1992_appointment: Option[Boolean],
                  links: Option[Links],
                  name: Option[String],
                  name_elements: Option[NameElements],
                  nationality: Option[String],
                  occupation: Option[String],
                  officer_role: Option[String],
                  principal_office_address: Option[PrincipalOfficeAddress],
                  resigned_on: Option[String],
                  responsibilities: Option[String]
                )

case class Address(
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

case class AppointedTo(
                        company_name: Option[String],
                        company_number: Option[String],
                        company_status: Option[String]
                      )

case class ContactDetails(
                           contact_name: Option[String]
                         )

case class FormerNames(
                        forenames: Option[String],
                        surname: Option[String]
                      )

case class Identification(
                           identification_type: Option[String],
                           legal_authority: Option[String],
                           legal_form: Option[String],
                           place_registered: Option[String],
                           registration_number: Option[String]
                         )

case class Links(
                  company: Option[String],
                  self: Option[String]
                )

case class NameElements(
                         forename: Option[String],
                         honours: Option[String],
                         other_forenames: Option[String],
                         surname: Option[String],
                         title: Option[String]
                       )

case class PrincipalOfficeAddress(
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




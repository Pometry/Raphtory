package com.raphtory.examples.coho.companiesStream.rawModel.officers

case class Officers(
                     resource_kind: Option[String],
                     resource_uri: Option[String],
                     resource_id: Option[String],
                     data: Option[Data],
                     event: Option[Event]
                   )

case class Data(
                 address: Option[Address],
                 appointed_on: Option[String],
                 contact_details: Option[ContactDetails],
                 country_of_residence: Option[String],
                 date_of_birth: Option[DateOfBirth],
                 former_names: Option[Array[FormerNames]],
                 identification: Option[Identification],
                 links: Option[Links],
                 name: Option[String],
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

case class ContactDetails(
                           contact_name: Option[String]
                         )

case class DateOfBirth(
                        day: Option[Int],
                        month: Option[Int],
                        year: Option[Int]
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
                  officer: Option[Officer],
                  self: Option[String]
                )

case class Officer(
                    appointments: Option[String]
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

case class Event(
                  fields_changed: Option[Array[String]],
                  published_at: Option[String],
                  timepoint: Option[String],
                  _type: Option[String]
                )

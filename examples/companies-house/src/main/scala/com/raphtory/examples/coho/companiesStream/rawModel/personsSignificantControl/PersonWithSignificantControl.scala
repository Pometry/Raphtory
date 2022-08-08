package com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl

case class PersonWithSignificantControl(
                                       active_count: Option[Int],
                                       ceased_count: Option[Int],
                                       items: Option[List[Items]],
                                       items_per_page: Option[Int],
                                       links: Option[Links],
                                       start_index: Option[Int],
                                       total_results: Option[Int]
                                       )

case class Items(
                address: Option[Address],
                ceased: Option[Boolean],
                ceased_on: Option[String],
                country_of_residence: Option[String],
                date_of_birth: Option[DateOfBirth],
                description: Option[String],
                etag: Option[String],
                identification: Option[Identification],
                is_sanctioned: Option[String],
                kind: Option[String],
                links: Option[Links1],
                name: Option[String],
                name_elements: Option[NameElements],
                nationality: Option[String],
                natures_of_control: Option[List[String]],
                notified_on: Option[String],
                principal_office_address: Option[PrincipalOfficeAddress]
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

case class DateOfBirth(
                       day: Option[Int],
                       month: Option[Int],
                       year: Option[Int]
                       )

case class Identification(
                           country_registered: Option[String],
                           legal_authority: Option[String],
                           legal_form: Option[String],
                           place_registered: Option[String],
                           registration_number: Option[String]
                         )

case class Links(
                  self: Option[String],
                  persons_with_significant_control_list: Option[String]
                )

case class Links1(
                  self: Option[String],
                  statement: Option[String]
                )

case class NameElements(
                       forename: Option[String],
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
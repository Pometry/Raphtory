package com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl

case class PersonWithSignificantControlItem(
                                           address: Option[Address],
                                           ceased: Option[Boolean],
                                           ceased_on: Option[String],
                                           country_of_residence: Option[String],
                                           date_of_birth: Option[DateOfBirth],
                                           description: Option[String],
                                           etag: Option[String],
                                           identification: Option[Identification],
                                           is_sanctioned: Option[Boolean],
                                           kind: Option[String],
                                           links: Option[Links],
                                           name: Option[String],
                                           name_elements: Option[NameElements],
                                           nationality: Option[String],
                                           natures_of_control: Option[List[String]],
                                           notified_on: Option[String],
                                           principal_office_address: Option[PrincipalOfficeAddress]
                                           )

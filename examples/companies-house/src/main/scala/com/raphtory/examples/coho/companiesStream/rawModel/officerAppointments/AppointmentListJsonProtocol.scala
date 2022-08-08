package com.raphtory.examples.coho.companiesStream.rawModel.officerAppointments

import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}

object AppointmentListJsonProtocol extends DefaultJsonProtocol {

  implicit val dateOfBirthFormat = jsonFormat2(DateOfBirth)
  implicit val addressFormat = jsonFormat9(Address)
  implicit val appointToFormat = jsonFormat3(AppointedTo)
  implicit val contactDetailsFormat = jsonFormat1(ContactDetails)
  implicit val formerNamesFormat = jsonFormat2(FormerNames)
  implicit val identificationFormat = jsonFormat5(Identification)
  implicit val linksFormat = jsonFormat2(Links)
  implicit val nameElementsFormat = jsonFormat5(NameElements)
  implicit val principalOfficeAddress = jsonFormat9(PrincipalOfficeAddress)

  def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
    jsObj.getFields(field).headOption

  def getField(field: String)(implicit jsObj: JsObject): Option[String] =
    getRawField(field) match {
      case Some(s) => Some(s.toString())
      case None => None
    }

  def getInteger(field: String)(implicit jsObj: JsObject): Option[Integer] =
    getRawField(field) match {
      case Some(s) => Some(s.toString().toInt)
      case None => None
    }

  def getBoolean(field: String)(implicit jsObj: JsObject): Option[Boolean] =
    getRawField(field) match {
      case Some(s) => Some(s.toString().toBoolean)
      case None => None
    }

  implicit object ItemsFormat extends RootJsonFormat[Items] {

    override def write(obj: Items): JsValue = JsString("TODO")
    override def read(json: JsValue): Items = {
      implicit val jsObj = json.asJsObject()
      Items(
        getRawField("address") match {
          case Some(a) => Some(a.convertTo[Address])
          case None => None
        },
        getField("appointed_before"),
        getField("appointed_on"),
        getRawField("appointed_to") match {
          case Some(at) => Some(at.convertTo[AppointedTo])
          case None => None
        },
        getRawField("contact_details") match {
          case Some(cd) => Some(cd.convertTo[ContactDetails])
          case None => None
        },
        getField("country_of_residence"),
        getRawField("former_names") match {
          case Some(fn) => Some(fn.convertTo[List[FormerNames]])
          case None => None
        },
        getRawField("identification") match {
          case Some(i) => Some(i.convertTo[Identification])
          case None => None
        },
        getBoolean("is_pre_1992_appointment"),
        getRawField("links") match {
          case Some(l) => Some(l.convertTo[Links])
          case None => None
        },
        getField("name"),
        getRawField("name_elements") match {
          case Some(ne) => Some(ne.convertTo[NameElements])
          case None => None
        },
        getField("nationality"),
        getField("occupation"),
        getField("officer_role"),
        getRawField("principal_office_address") match {
          case Some(poa) => Some(poa.convertTo[PrincipalOfficeAddress])
          case None => None
        },
        getField("resigned_on"),
        getField("responsibilities")
      )
    }
  }

  implicit object OfficerAppointmentListFormat extends RootJsonFormat[OfficerAppointmentList] {

    override def write(obj: OfficerAppointmentList): JsValue = JsString("TODO")
    override def read(json: JsValue): OfficerAppointmentList = {
      implicit val jsObj = json.asJsObject
      OfficerAppointmentList(
        getRawField("date_of_birth") match {
          case Some(dob) => Some(dob.convertTo[DateOfBirth])
          case None => None
        },
        getField("etag"),
        getBoolean("is_corporate_officer"),
        getRawField("items") match {
          case Some(i) => Some(i.convertTo[List[Items]])
          case None => None
        },
        getField("items_per_page"),
        getField("kind"),
        getRawField("links") match {
          case Some(l) => Some(l.convertTo[Links])
          case None => None
        },
        getField("name"),
        getField("start_index"),
        getField("total_results")
      )
    }
  }
}
package com.raphtory.examples.coho.companiesStream.rawModel.officers

import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat}

object OfficersJsonProtocol extends DefaultJsonProtocol {
  implicit val eventFormat = jsonFormat4(Event)
  implicit val addressFormat = jsonFormat9(Address)
  implicit val contactDetailsFormat = jsonFormat1(ContactDetails)
  implicit val dateOfBirthFormat = jsonFormat3(DateOfBirth)
  implicit val formerNamesFormat = jsonFormat2(FormerNames)
  implicit val identificationFormat = jsonFormat5(Identification)
  implicit val officerFormat = jsonFormat1(Officer)
  implicit val principalOfficeAddressFormat = jsonFormat9(PrincipalOfficeAddress)

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

  implicit object LinksFormat extends RootJsonFormat[Links] {
    override def write(obj: Links): JsValue = JsString("TODO")
    override def read(json: JsValue): Links = {
      implicit val jsObj = json.asJsObject
      Links(
        getRawField("officer") match {
          case Some(o) => Some(o.convertTo[Officer])
          case None => None
        },
        getField("self")
      )
    }
  }

  implicit object DataFormat extends RootJsonFormat[Data] {
    override def write(obj: Data): JsValue = JsString("TODO")
    override def read(json: JsValue): Data = {
      implicit val jsObj = json.asJsObject
      Data(
        getRawField("address") match {
          case Some(a) => Some(a.convertTo[Address])
          case None => None
        },
        getField("appointed_on"),
        getRawField("contact_details") match {
          case Some(cd) => Some(cd.convertTo[ContactDetails])
          case None => None
        },
        getField("country_of_residence"),
        getRawField("date_of_birth") match {
          case Some(dob) => Some(dob.convertTo[DateOfBirth])
          case None => None
        },
        getRawField("former_names") match {
          case Some(fn) => Some(fn.convertTo[Array[FormerNames]])
          case None => None
        },
        getRawField("identification") match {
          case Some(i) => Some(i.convertTo[Identification])
          case None => None
        },
        getRawField("links") match {
          case Some(l) => Some(l.convertTo[Links])
          case None => None
        },
        getField("name"),
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



  implicit object OfficersFormat extends RootJsonFormat[Officers] {

    override def write(obj: Officers): JsValue = JsString("TODO")

    override def read(json: JsValue): Officers = {
      implicit val jsObj = json.asJsObject
      Officers(
        getField("resource_kind"),
        getField("resource_uri"),
        getField("resource_id"),
        getRawField("data") match {
          case Some(d) => Some(d.convertTo[Data])
          case None => None
        },
        getRawField("event") match {
          case Some(e) => Some(e.convertTo[Event])
          case None => None
        }
      )
    }
  }


}

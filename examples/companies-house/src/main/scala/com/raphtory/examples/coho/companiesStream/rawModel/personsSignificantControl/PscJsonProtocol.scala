package com.raphtory.examples.coho.companiesStream.rawModel.personsSignificantControl

import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat}

object PscJsonProtocol extends DefaultJsonProtocol {

  implicit val linksFormat = jsonFormat2(Links)
  implicit val links1Format = jsonFormat2(Links1)
  implicit val addressFormat = jsonFormat9(Address)
  implicit val dateOfBirthFormat = jsonFormat3(DateOfBirth)
  implicit val identificationFormat = jsonFormat5(Identification)
  implicit val nameElementsFormat = jsonFormat4(NameElements)
  implicit val principalOfficeAddressFormat = jsonFormat9(PrincipalOfficeAddress)

  def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
    jsObj.getFields(field).headOption

  def getField(field: String)(implicit jsObj: JsObject): Option[String] =
    getRawField(field) match {
      case Some(s) => Some(s.toString())
      case None => None
    }

  def getInteger(field: String)(implicit jsObj: JsObject): Option[Int] =
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
        getBoolean("ceased"),
        getField("ceased_on"),
        getField("country_of_residence"),
        getRawField("date_of_birth") match {
          case Some(at) => Some(at.convertTo[DateOfBirth])
          case None => None
        },
        getField("description"),
        getField("etag"),
        getRawField("identification") match {
          case Some(cd) => Some(cd.convertTo[Identification])
          case None => None
        },
        getField("is_sanctioned"),
        getField("kind"),
        getRawField("links") match {
          case Some(fn) => Some(fn.convertTo[Links1])
          case None => None
        },
        getField("name"),
        getRawField("name_elements") match {
          case Some(i) => Some(i.convertTo[NameElements])
          case None => None
        },
        getField("nationality"),
        getRawField("natures_of_control") match {
          case Some(i) => Some(i.convertTo[List[String]])
          case None => None
        },
        getField("notified_on"),
        getRawField("principal_office_address") match {
          case Some(i) => Some(i.convertTo[PrincipalOfficeAddress])
          case None => None
        }
      )
    }
  }

  implicit object PscFormat extends RootJsonFormat[PersonWithSignificantControl] {

    override def write(obj: PersonWithSignificantControl): JsValue = JsString("TODO")
    override def read(json: JsValue): PersonWithSignificantControl = {
      implicit val jsObj = json.asJsObject
      PersonWithSignificantControl(
        getInteger("active_count"),
        getInteger("ceased_count"),
        getRawField("items") match {
          case Some(i) => Some(i.convertTo[List[Items]])
          case None => None
        },
        getInteger("items_per_page"),
        getRawField("links") match {
          case Some(l) => Some(l.convertTo[Links])
          case None => None
        },
        getInteger("start_index"),
        getInteger("total_results")
      )
    }
  }
}

package com.raphtory.examples.oag

import java.net.URL
import spray.json.JsNumber

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}
;

object OpenAcademicJsonProtocol extends DefaultJsonProtocol {

  implicit object OpenAcademicDocJsonFormat extends RootJsonFormat[OpenAcademic] {
    // TODO Writer method

    def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
      jsObj.getFields(field).headOption

    def getField(field: String)(implicit jsObj: JsObject): Option[String] =
      getRawField(field) match {
        case Some(s) => Some(s.toString())
        case None => None
      }

    def getReferences(field: String)(implicit jsObj: JsObject): Option[List[OpenAcademic]] = {
      val refs = mutable.MutableList[OpenAcademic]()
      val raw = getRawField(field)
      if (raw == None) {
//        return new List[]
        return None
      }
      val sValue =  raw.get
      val sArray = sValue.asInstanceOf[JsArray]
      for(r <- sArray.elements) {
        refs+=read(r)
      }
      Some(refs.toList)
    }

    def getBoolean(field: String)(implicit jsObj: JsObject): Option[Boolean] =
      getField(field) match {
        case Some(s) => Some(s.toBoolean)
        case None => None
      }

    def getInt(field: String)(implicit jsObj: JsObject): Option[Int] =
      getField(field) match {
        case Some(s) => Some(s.toInt)
        case None => None
      }

    def getLong(field: String)(implicit jsObj: JsObject): Option[Long] =
      getField(field) match {
        case Some(s) => Some(s.toLong)
        case None => None
      }

    def write(p: OpenAcademic) = JsString("TODO")

    def read(value: JsValue) = {
        val jsObj = value.asJsObject
        //val entitiesArray = jsObj.fields.get("entities")

        var raw_attributes = jsObj
        /*if (entitiesArray != None) {
          val jsonEntities =  entitiesArray.get
          val en = jsonEntities.asInstanceOf[JsArray]
          raw_attributes = en.elements(0).asJsObject
        }*/
//        implicit val attributes = en.elements(0).asJsObject
        implicit val attributes = raw_attributes

            new OpenAcademic(
              getField("title") match {
                case Some(s) => Some(s.replaceAll("\\\\", "").replaceAll("\"", ""))
                case None    => None
              },
              getField("doi") match {
                case Some(s) => Some(s.replaceAll("\"", ""))
                case None    => None
              },
              getField("paperId") match {
                case Some(s) => Some(s.replaceAll("\"", ""))
                case None    => None
              },
              getInt("year"),
              getReferences("references"),//references
              getReferences("citations"),//citations
//              getBoolean("is_seed"),
              getBoolean("isSeed"),
              getField("labelDensity") match {
                case Some(s) => Some(s.toDouble)
                case None    => None
              },
            )
    }
  }
}

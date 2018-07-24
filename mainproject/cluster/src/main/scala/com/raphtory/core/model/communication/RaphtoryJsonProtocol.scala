package com.raphtory.core.model.communication

import com.raphtory.core.utils.CommandEnum
import spray.json._

object RaphtoryJsonProtocol extends DefaultJsonProtocol {
  implicit val vertexAddWithPropertiesFormat = jsonFormat3(VertexAddWithProperties)
  implicit val edgeAddWithPropertiesFormat   = jsonFormat4(EdgeAddWithProperties)
  implicit val vertexAddFormat               = jsonFormat2(VertexAdd)
  implicit val edgeAddFormat                 = jsonFormat3(EdgeAdd)

  implicit object raphCaseClassFormat extends RootJsonFormat[RaphWriteClass] {
    def write(obj: RaphWriteClass): JsValue =
      JsObject((obj match {
        case e : VertexAddWithProperties => e.toJson
        case e : EdgeAddWithProperties => e.toJson
        case e : VertexAdd => e.toJson
        case e : EdgeAdd => e.toJson
      }).asJsObject.fields)

    def read(json: JsValue) = null // TODO
  }

  implicit object commandFormat extends RootJsonFormat[Command] {
    def write(obj : Command) : JsValue =
      JsObject(obj.command.toString -> obj.value.toJson)
    def read(json: JsValue) = {
      val head = json.asJsObject.fields.head
      head._1 match {
        case "vertexAdd" => Command(CommandEnum.withName(head._1), head._2.convertTo[VertexAdd])
        case "edgeAdd"   => Command(CommandEnum.withName(head._1), head._2.convertTo[EdgeAdd])
        case "vertexAddWithProperties" => Command(CommandEnum.withName(head._1), head._2.convertTo[VertexAddWithProperties])
        case "edgeAddWithProperties" => Command(CommandEnum.withName(head._1), head._2.convertTo[EdgeAddWithProperties])
      }
    }
  }
}

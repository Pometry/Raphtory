package com.raphtory.core.model.communication

import com.raphtory.core.utils.CommandEnum
import spray.json._

object RaphtoryJsonProtocol extends DefaultJsonProtocol {
  implicit val vertexAddWithPropertiesFormat = jsonFormat3(VertexAddWithProperties)
  implicit val edgeAddWithPropertiesFormat = jsonFormat4(EdgeAddWithProperties)

  implicit object raphCaseClassFormat extends RootJsonFormat[RaphCaseClass] {
    def write(obj: RaphCaseClass): JsValue =
      JsObject((obj match {
        case e : VertexAddWithProperties => e.toJson
        case e : EdgeAddWithProperties => e.toJson
      }).asJsObject.fields)

    def read(json: JsValue) = null // TODO
  }

  implicit object commandFormat extends RootJsonFormat[Command] {
    def write(obj : Command) : JsValue =
      JsObject(obj.command.toString -> obj.value.toJson)
    def read(json: JsValue) = {
      val head = json.asJsObject.fields.head
      head._1 match {
        case "vertexAdd" => Command(CommandEnum.withName(head._1), head._2.convertTo[VertexAddWithProperties])
        case "edgeAdd"   => Command(CommandEnum.withName(head._1), head._2.convertTo[EdgeAddWithProperties])
      }
    }
  }
}

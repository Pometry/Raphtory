package com.raphtory.Actors.RaphtoryActors.Router

import spray.json.JsObject

trait RouterTrait {
  def parseJSON(command : String) : Unit

  def vertexAdd(command : JsObject) : Unit
  def vertexUpdateProperties(command : JsObject) : Unit
  def vertexRemoval(command : JsObject) : Unit

  def edgeAdd(command : JsObject) : Unit
  def edgeUpdateProperties(command : JsObject) : Unit
  def edgeRemoval(command : JsObject) : Unit
}

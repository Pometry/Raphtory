package com.raphtory.core.implementations.generic.entity.internal

import scala.collection.mutable

trait InternalEntity {
  def getType():String
  def properties():mutable.Map[String, InternalProperty]
  def history(): mutable.TreeMap[Long, Boolean]
  def aliveAt(time: Long): Boolean
  def aliveAtWithWindow(time: Long, windowSize: Long): Boolean
}

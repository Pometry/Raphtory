package com.raphtory.core.algorithm

import scala.reflect.runtime.universe._

class Row(values: Array[Any]) {
  def apply(index: Int): Any           = values(index)
  def get(index: Int): Any             = values(index)
  def getAs[T: TypeTag](index: Int): T = values(index).asInstanceOf[T]
  def getInt(index: Int): Int          = getAs[Int](index)
  def getString(index: Int): String    = getAs[String](index)
  def getBool(index: Int): Boolean     = getAs[Boolean](index)
  def getLong(index: Int): Long        = getAs[Long](index)
  def getDouble(index: Int): Double    = getAs[Double](index)
  def getValues(): Array[Any]          = values
}

object Row {

  //def apply(values: Array[Any]): Row = new Row(values)
  def apply(values: Any*): Row = {
    val valuesArray = values.toArray
    new Row(valuesArray)
  }
}

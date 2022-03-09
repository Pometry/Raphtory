package com.raphtory.algorithms.api

/**
  * {s}`Row(values: Any*)`
  *    : Create a row of a data table
  *
  * ## Parameters
  *
  *  {s}`values: Any*`
  *    : Values to store in row
  *
  * ## Methods
  *
  *  {s}`apply(index: Int): Any`
  *    : Return value at `index`
  *
  *  {s}`get(index: Int): Any`
  *    : same as `apply`
  *
  *  {s}`getAs[T](index: Int): T`
  *      : Return value at `index` and cast it to type `T`
  *
  *  {s}`getInt(index: Int): Int`
  *    : Same as {s}`getAs[Int](index)`
  *
  *  {s}`getString(index: Int): String`
  *    : Same as {s}`getAs[String](index)`
  *
  *  {s}`getBool(index: Int): Boolean`
  *    : Same as {s}`getAs[Boolean](index)`
  *
  *  {s}`getLong(index: Int): Long`
  *    : Same as {s}`getAs[Long](index)`
  *
  *  {s}`getDouble(index: Int): Double`
  *    : Same as {s}`getAs[Double](index)`
  *
  *  {s}`getValues(): Array[Any]`
  *    : Return Array of values
  *
  *  ```{seealso}
  *  [](com.raphtory.algorithm.api.Table)
  *  ```
  */
class Row(values: Array[Any]) {
  def apply(index: Int): Any        = values(index)
  def get(index: Int): Any          = values(index)
  def getAs[T](index: Int): T       = values(index).asInstanceOf[T]
  def getInt(index: Int): Int       = getAs[Int](index)
  def getString(index: Int): String = getAs[String](index)
  def getBool(index: Int): Boolean  = getAs[Boolean](index)
  def getLong(index: Int): Long     = getAs[Long](index)
  def getDouble(index: Int): Double = getAs[Double](index)
  def getValues(): Array[Any]       = values
}

object Row {

  //def apply(values: Array[Any]): Row = new Row(values)
  def apply(values: Any*): Row = {
    val valuesArray = values.toArray
    new Row(valuesArray)
  }
}

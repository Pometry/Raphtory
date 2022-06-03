package com.raphtory.api.table

/** Create a row of a data table
  *
  * @param values to store in row
  *  @see [[Table]]
  */
class Row(values: Array[Any]) {

  /** Return value at index
    * @param index index to obtain value from
    */
  def apply(index: Int): Any = values(index)

  /** Return value at index */
  def get(index: Int): Any = values(index)

  /** Return value at `index` and cast it to type `T` */
  def getAs[T](index: Int): T = values(index).asInstanceOf[T]

  /** Same as `getAs[Int](index)` */
  def getInt(index: Int): Int = getAs[Int](index)

  /** Same as `getAs[String](index)` */
  def getString(index: Int): String = getAs[String](index)

  /** Same as `getAs[Boolean](index)` */
  def getBool(index: Int): Boolean = getAs[Boolean](index)

  /** Same as `getAs[Long](index)` */
  def getLong(index: Int): Long = getAs[Long](index)

  /** Same as `getAs[Double](index)` */
  def getDouble(index: Int): Double = getAs[Double](index)

  /** Return Array of values */
  def getValues(): Array[Any] = values
}

object Row {

  //def apply(values: Array[Any]): Row = new Row(values)
  def apply(values: Any*): Row = {
    val valuesArray = values.toArray
    new Row(valuesArray)
  }
}

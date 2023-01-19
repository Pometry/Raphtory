package com.raphtory.api.analysis.table

import com.raphtory.internals.components.querymanager.ProtoField

/** Create a row of a data table
  * @see [[Table]]
  */

case class KeyPair(key: String, value: Any)

class Row(val columns: Map[String, Any]) { // TODO: decide which Map implementation we are gonna use

  /** Return value at index
    * @param index index to obtain value from
    */
  def apply(key: String): Any = columns(key)

  /** Return value at `index` */
  def get(key: String): Any = columns(key)

  /** Return value at `index` and cast it to type `T` */
  def getAs[T](key: String): T = columns(key).asInstanceOf[T]

  /** Same as `getAs[Int](index)` */
  def getInt(key: String): Int = getAs[Int](key)

  /** Same as `getAs[String](index)` */
  def getString(key: String): String = getAs[String](key)

  /** Same as `getAs[Boolean](index)` */
  def getBool(key: String): Boolean = getAs[Boolean](key)

  /** Same as `getAs[Long](index)` */
  def getLong(key: String): Long = getAs[Long](key)

  /** Same as `getAs[Double](index)` */
  def getDouble(key: String): Double = getAs[Double](key)

//  def insertDictintoKeypair(dictionary: Map[String, Any]): Array[KeyPair] =
//    dictionary.map {
//      case (key: String, value: Any) => KeyPair(key, value)
//      case _                         => throw new IllegalArgumentException("Keypairs in Row should be of type (key: String, value: Any)")
//    }.toArray

  /** Return Array of values */
  def values(): Array[Any] = columns.values.toArray

  /** Return Array of keys */
  def keys(): Array[String] = columns.keys.toArray

  override def toString: String = "Row(" + values.mkString(", ") + ")"

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Row =>
        that.values.toSeq == this.values.toSeq
      case _         => false
    }

  override def hashCode(): Int = values.toSeq.hashCode()
}

/** Factory object for Rows */
object Row extends ProtoField[Row] {

  /** Create a new Row object */
  def apply(values: Map[String, Any]): Row = new Row(values)
}

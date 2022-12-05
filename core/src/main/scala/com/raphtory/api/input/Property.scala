package com.raphtory.api.input

/** Properties are characteristic attributes like name, etc. assigned to Vertices and Edges by the
  * [[Graph]].
  *
  * @see [[Graph]]
  */

/** Sealed trait defining different types of properties */
sealed trait Property {

  /** property name */
  def key: String

}

/** Vertex/Edge type (this is not a `Property`) */
sealed trait MaybeType {
  def toOption: Option[Type]
}

object NoType extends MaybeType {
  override def toOption: Option[Type] = None
}

case class Type(name: String) extends MaybeType {
  override def toOption: Option[Type] = Some(this)
}

/** `Property` with a fixed value (the value should be the same for each update to the entity) */
case class ImmutableString(key: String, value: String) extends Property

/** `Property` with a `String` value */
case class MutableString(key: String, value: String) extends Property

/** `Property` with a `Long` value */
case class MutableLong(key: String, value: Long) extends Property

/** `Property` with a `Double` value */
case class MutableDouble(key: String, value: Double) extends Property

/** `Property` with a `Float` value */
case class MutableFloat(key: String, value: Float) extends Property

/** `Property` with a `Boolean` value */
case class MutableBoolean(key: String, value: Boolean) extends Property

/** `Property` with a `Integer` value */
case class MutableInteger(key: String, value: Int) extends Property

/** Wrapper class for properties */
case class Properties(properties: Property*) {

  def apply(properties: List[Property]): Properties =
    Properties(properties: _*)

  def addProperty(property: Property): Properties =
    Properties(properties.:+(property): _*)
}

object Properties {
  val empty: Properties = Properties()
}

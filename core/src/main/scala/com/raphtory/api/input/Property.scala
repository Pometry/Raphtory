package com.raphtory.api.input

/** Properties are characteristic attributes like name, etc. assigned to Vertices and Edges by the
  * [[Graph Builder]].
  *
  * @see [[GraphBuilder]]
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
case class ImmutableProperty(key: String, value: String) extends Property

/** `Property` with a `String` value */
case class StringProperty(key: String, value: String) extends Property

/** `Property` with a `Long` value */
case class LongProperty(key: String, value: Long) extends Property

/** `Property` with a `Double` value */
case class DoubleProperty(key: String, value: Double) extends Property

/** `Property` with a `Float` value */
case class FloatProperty(key: String, value: Float) extends Property

/** `Property` with a `Boolean` value */
case class BooleanProperty(key: String, value: Boolean) extends Property

/** `Property` with a `Integer` value */
case class IntegerProperty(key: String, value: Integer) extends Property

/** Wrapper class for properties */
case class Properties(properties: Vector[Property])

object Properties {
  def apply(property: Property*): Properties = Properties(Vector.from(property))

  def apply(properties: IterableOnce[Property]): Properties = Properties(Vector.from(properties))
}

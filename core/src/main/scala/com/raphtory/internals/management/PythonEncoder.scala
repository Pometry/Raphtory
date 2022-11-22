package com.raphtory.internals.management

import java.util
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Success
import scala.util.Try

trait PythonEncoder[A] extends Serializable {
  def encode(a: A): Object

  def decode(pyObj: Object): A

  def clz: Class[_]
}

object PythonEncoder {

  def apply[T](implicit T: PythonEncoder[T]): PythonEncoder[T] = T

  implicit val strEncoder: PythonEncoder[String] =
    createEncoder[String](s => s, pyo => pyo.asInstanceOf[String], classOf[String])

  implicit val intEncoder: PythonEncoder[Int] =
    createEncoder[Int](int => Int.box(int), pyint => pyint.asInstanceOf[Integer], classOf[Integer])

  implicit val longEncoder: PythonEncoder[Long] =
    createEncoder[Long](int => Long.box(int), pylong => pylong.asInstanceOf[java.lang.Long], classOf[java.lang.Long])

  implicit val booleanEncoder: PythonEncoder[Boolean] =
    createEncoder[Boolean](
            int => Boolean.box(int),
            pylong => pylong.asInstanceOf[java.lang.Boolean],
            classOf[java.lang.Boolean]
    )

  implicit val integerEncoder: PythonEncoder[Integer] =
    createEncoder[Integer](int => int, pylong => pylong.asInstanceOf[java.lang.Integer], classOf[java.lang.Integer])

  implicit val doubleEncoder: PythonEncoder[Double] =
    createEncoder[Double](
            int => Double.box(int),
            pylong => pylong.asInstanceOf[java.lang.Double],
            classOf[java.lang.Double]
    )

  implicit val floatEncoder: PythonEncoder[Float] =
    createEncoder[Float](
            int => Float.box(int),
            pylong => pylong.asInstanceOf[java.lang.Float],
            classOf[java.lang.Float]
    )

  implicit def optEncoder[T](implicit PE: PythonEncoder[T]): PythonEncoder[Option[T]] =
    createEncoder[Option[T]](
            {
              case None    => null
              case Some(t) => PE.encode(t)
            },
            {
              case null => None
              case t    => Option(PE.decode(t))
            },
            PE.clz
    )

  implicit def vecEncode[T: PythonEncoder: ClassTag]: PythonEncoder[Vector[T]] =
    createEncoder[Vector[T]](
            vec => vec.map(t => PythonEncoder[T].encode(t)).toArray,
            pyObj => {
              val objects = pyObj.asInstanceOf[Array[Object]]
              val ts      = objects.map(obj => PythonEncoder[T].decode(obj))
              ts.toVector
            },
            classOf[Array[Object]]
    )

  def createEncoder[A](read: A => Object, write: Object => A, cls: Class[_]): PythonEncoder[A] =
    new PythonEncoder[A] {
      override def encode(a: A): Object = read(a)

      override def decode(pyObj: Object): A = write(pyObj)

      override def clz: Class[_] = cls
    }

  // ******* TYPE CLASS DERIVATION ******* //
  import magnolia1._

  import language.experimental.macros
  type Typeclass[T] = PythonEncoder[T]

  def join[T](ctx: CaseClass[PythonEncoder, T]): PythonEncoder[T] =
    new Typeclass[T] {

      override def encode(a: T): Object =
        ctx.parameters.foldLeft(new util.HashMap[String, Object]()) { (hm, param) =>
          hm.put(param.label, param.typeclass.encode(param.dereference(a)))
          hm
        }

      override def decode(pyObj: Object): T = {
        val pyO = pyObj.asInstanceOf[java.util.HashMap[String, Object]]
        ctx.construct { p =>
          val value = pyO.get(camelToSnakeCase(p.label))
          assert(value != null, s"cannot find key ${p.label} within ${pyO.keySet()}")
          p.typeclass.decode(value)
        }
      }

      override def clz: Class[_] = classOf[java.util.HashMap[String, Object]]
    }

  def camelToSnakeCase(input: String): String = {
    if (input == null) return input // garbage in, garbage out
    val length            = input.length
    val result            = new mutable.StringBuilder(length * 2)
    var resultLength      = 0
    var wasPrevTranslated = false
    for (i <- 0 until length) {
      var c = input.charAt(i)
      if (c == '$') c = '_'
      if (Character.isUpperCase(c)) {
        if (!wasPrevTranslated && resultLength > 0 && result.charAt(resultLength - 1) != '_') {
          result.append('_')
          resultLength += 1
        }
        c = Character.toLowerCase(c)
        wasPrevTranslated = true
      }
      else wasPrevTranslated = false
      result.append(c)
      resultLength += 1

    }
    if (resultLength > 0) result.toString
    else input
  }

  def split[T](ctx: SealedTrait[PythonEncoder, T]): PythonEncoder[T] =
    new Typeclass[T] {

      override def encode(a: T): AnyRef =
        ctx.split(a) { sub =>
          sub.typeclass.encode(sub.cast(a)) match {
            case hm: util.HashMap[String, Object] @unchecked =>
              hm.put("_type", sub.typeName.full)
          }
        }

      override def decode(pyObj: Object): T =
        pyObj match {
          case hm: util.HashMap[String, Object] @unchecked =>
            val name = hm.get("_type").asInstanceOf[String]
            ctx.subtypes.find(_.typeName.full == name) match {
              case Some(st) =>
                st.typeclass.decode(pyObj)
              case None     => // TRY EVERYTHING
                val maybe = ctx.subtypes
                  .map { st =>
                    Try {
                      st.typeclass.decode(pyObj)
                    }
                  }
                  .collectFirst { case Success(value) => value }
                maybe match {
                  case Some(v) => v
                  case None    =>
                    throw new IllegalArgumentException(s"Unable to pick sealed trait variant $hm")
                }
            }
        }

      override def clz: Class[_] = classOf[java.util.HashMap[String, Object]]
    }

  implicit def gen[T]: PythonEncoder[T] = macro Magnolia.gen[T]

}

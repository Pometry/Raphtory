package com.raphtory.internals.management

import cats.syntax.all._
import cats.Applicative
import cats.Monad
import cats.effect.Async
import cats.effect.Resource
import pemja.core.PythonInterpreter
import pemja.core.PythonInterpreterConfig

import java.nio.file.Path
import java.util
import scala.reflect.ClassTag

class PythonEmbedded[IO[_]](py: PythonInterpreter, private var i: Int = 0)(implicit IO: Async[IO]) {

  def loadGraphBuilder(cls: String, pkg: String): IO[PyRef] =
    IO.blocking {
      py.exec(s"from $pkg import $cls")
      val name = s"pyref_$i"
      i += 1
      py.exec(s"$name = $cls()")
      PyRef(name)
    }

  private[management] def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty) =
    IO.blocking {
      py.invokeMethod(ref.name, methodName, args: _*)
    }

  private[management] def get[T](expr: String, clz: Class[T]): IO[T] =
    IO.blocking {
      val name = s"tmp_$i"
      i += 1
      py.exec(s"$name = $expr; print($name)")
      py.get(name, clz)
    }

  private[management] def getT[T](expr: String)(implicit PE: PythonEncoder[T]): IO[T] =
    IO.blocking {
      val name  = s"tmp_$i"
      i += 1
      py.exec(s"$name = $expr; print($name)")
      val pyObj = py.get(name, PE.clz.asSubclass(classOf[Object]))
      val a = PE.decode(pyObj)
      a
    }

}

object PythonEmbedded {

  def apply[IO[_]: Async](pythonPaths: Path*): Resource[IO, PythonEmbedded[IO]] =
    for {
      config <- Resource.eval(Async[IO].blocking {
                  pythonPaths
                    .foldLeft(PythonInterpreterConfig.newBuilder()) { (b, path) =>
                      b.addPythonPaths(path.toAbsolutePath.toString)
                    }
                    .setPythonExec("/home/murariuf/.virtualenvs/raphtory/bin/python3")
                    .build()
                })
      py     <- Resource.fromAutoCloseable(Async[IO].blocking(new PythonInterpreter(config))).map(new PythonEmbedded(_))
    } yield py
}

/**
  * Reference of object inside python
  * @param name
  * name of variable inside the python context
  */
case class PyRef(name: String)

class PythonGraphBuilder[IO[_]: Monad](ref: PyRef, py: PythonEmbedded[IO]) {

  def parseTuple(line: String) =
    py.invoke(ref, "parse_tuple", Vector(line)) *>
      py.getT[Vector[Person]](s"${ref.name}.get_actions()")

}

object PythonGraphBuilder {

  def apply[IO[_]: Monad](ref: PyRef, py: PythonEmbedded[IO]): IO[PythonGraphBuilder[IO]] =
    Applicative[IO].pure(new PythonGraphBuilder[IO](ref, py))
}

trait PythonEncoder[A] {
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

  implicit def vecEncode[T: PythonEncoder: ClassTag]: PythonEncoder[Vector[T]] =
    createEncoder[Vector[T]](
            vec => vec.map(t => PythonEncoder[T].encode(t)).toArray,
            pyObj => {
              val objects = pyObj.asInstanceOf[Array[Object]]
              val ts = objects.map(obj => PythonEncoder[T].decode(obj))
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
  import language.experimental.macros
  import magnolia1._
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
        ctx.construct(p => p.typeclass.decode(pyO.get(p.label)))
      }

      override def clz: Class[_] = classOf[java.util.HashMap[String, Object]]
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
            val name    = hm.get("_type").asInstanceOf[String]
            val subtype = ctx.subtypes.find(_.typeName.full == name).get
            subtype.typeclass.decode(pyObj)
        }

      override def clz: Class[_] = classOf[java.util.HashMap[String, Object]]
    }

  implicit def gen[T]: PythonEncoder[T] = macro Magnolia.gen[T]

}

case class Person(name: String, age: Long)


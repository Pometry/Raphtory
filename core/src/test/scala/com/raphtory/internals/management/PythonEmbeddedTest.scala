package com.raphtory.internals.management

import cats.effect.IO
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.typesafe.config.Config
import munit.CatsEffectSuite

import java.nio.file.Paths
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.SeqHasAsJava

class PythonEmbeddedTest extends CatsEffectSuite {
//
//  test("can wrap POJO vertex inside python".only) {
//    PythonEmbedded[IO](
//            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory"),
//            Paths.get("/home/murariuf/.virtualenvs/raphtory/lib/python3.8/site-packages")
//    ).use { py: PythonEmbedded[IO] =>
//      for {
//        jvmV     <- IO.pure(makeMeAVertex)
//        pyVertex <- py.set(new PythonVertex(jvmV))
//        _        <- py.withUnsafePy {
//                      _.exec {
//                        s"""from vertex import Vertex, Step
//                    |v = Vertex(${pyVertex.name})
//                    |s = Step()
//                    |s.eval(v)
//                    |""".stripMargin
//                      }
//                    }
//      } yield ()
//    }
//  }
//
//  private def makeMeAVertex: Vertex = {
//    val config: Config = new ConfigHandler().getConfig(false)
//    val vertex         = new PojoExVertex(
//            new PojoVertex(0, 0, true),
//            mutable.Map.empty,
//            mutable.Map.empty,
//            PojoGraphLens("1", 0, 200, 0, new PojoBasedPartition(0, config), null, null, null, null)
//    )
//
//    vertex.receiveMessage(VertexMessage(0, 0L, "blerg"))
//    vertex
//  }
//
//  test("can startup an embedded Python") {
//    PythonEmbedded[IO](
//            Paths.get("/pometry/Source/Raphtory/python/pyraphtory/pyraphtory"),
//            Paths.get("/home/murariuf/.virtualenvs/raphtory/lib/python3.8/site-packages")
//    ).use { py =>
//      for {
//        b      <- py.loadGraphBuilder("BaseBuilder", "builder")
//        actual <- b.parseTuple("Frodo,Sam,32666")
//        _      <- py.javaInterop { () =>
//                    println("HELLO CALLABLE!")
//                    "blerg"
//                  }
//      } yield assertEquals(
//              actual,
//              Vector(
//                      VertexAdd(
//                              32666,
//                              3491048503859410748L,
//                              Properties(ImmutableProperty("name", "Frodo")),
//                              Some(Type("Character"))
//                      ),
//                      VertexAdd(
//                              32666,
//                              -1395500071931009564L,
//                              Properties(ImmutableProperty("name", "Sam")),
//                              Some(Type("Character"))
//                      ),
//                      EdgeAdd(
//                              32666,
//                              3491048503859410748L,
//                              -1395500071931009564L,
//                              Properties(),
//                              Some(Type("Character Co-occurence"))
//                      )
//              )
//      )
//    }
//  }
//
//  override def munitTimeout: Duration = FiniteDuration(1, TimeUnit.DAYS)
}

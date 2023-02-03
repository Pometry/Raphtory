package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.arrowcore.model.PropertySchema
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap
import sun.jvm.hotspot.HelloWorld.e

import java.nio.file.Files
import scala.util.Using

class ArrowStorageSuite extends munit.FunSuite {

  private val defaultPropSchema = ArrowSchema[VertexProp, EdgeProp]

  test("add vertex with all types of properties and vertex type") {
    Using(mkPartition(1, 0, ArrowSchema[AllProps, EdgeProp])) {
      par =>
        val timestamp = System.currentTimeMillis()

        // add bob
        val now = System.currentTimeMillis()
        addVertex(
          3,
          timestamp,
          Some(Type("Person")),
          MutableLong("pLong", now),
          MutableDouble("pDouble", 1.234d),
          MutableFloat("pFloat", 4.321f),
          MutableBoolean("pBool", value = true),
          MutableString("pString", "blerg"),
          MutableInteger("pInt", 12345789),
          ImmutableString("name", "Bob")
        )(par)

        par.flush

        val actual = par.vertices.headOption.get

        assertEquals(actual.field[String]("name").get, Some("Bob"))
        assertEquals(actual.prop[Long]("pLong").get, Some(now))
        assertEquals(actual.prop[Double]("pDouble").get, Some(1.234d))
        assertEquals(actual.prop[Float]("pFloat").get, Some(4.321f))
        assertEquals(actual.prop[Boolean]("pBool").get, Some(true))
        assertEquals(actual.prop[Int]("pInt").get, Some(12345789))
        assertEquals(actual.prop[String]("pString").get, Some("blerg"))

        // one can iterate these values
        assertEquals(actual.prop[Long]("pLong").list.toList, List(now -> timestamp))
        // add one extra property
        addVertex(
          3,
          timestamp + 1,
          None,
          MutableLong("pLong", now + 1)
        )(par)
        par.flush

        val actual2 = par.vertices.head
        assertEquals(actual2.prop[Long]("pLong").list.toSet, Set(now -> timestamp, (now + 1, timestamp + 1)))
    }
  }

  test("add two vertices into partition") {

    Using(mkPartition(1, 0)){
      par =>
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, None, ImmutableString("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, None, ImmutableString("name", "Alice"))(par)

    par.flush

    val vs = par.vertices
    assertEquals(vs.size, 2)

    val names = vs.iterator.toList.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))

    assertEquals(names.toSet, Set(3L -> "Bob", 7L -> "Alice"))

    }
  }

  test("add a vertex 3 times and explore the history") {

    Using(mkPartition(1, 0)) {
      par =>
        val timestamp = System.currentTimeMillis()

        // add bob at t1
        val property = ImmutableString("name", "Bob")
        addVertex(3, timestamp, None, property)(par)
        // add bob at t3
        addVertex(3, timestamp + 2, None, property)(par)
        // add bob at t2
        addVertex(3, timestamp + 1, None, property)(par)

        par.flush()
        val vs = par.vertices
        assertEquals(vs.size, 1)

        val names = vs.iterator.toList.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))

        assertEquals(names, List(3L -> "Bob"))

        val bob = par.vertices.headOption.get

        assertEquals(
          bob.history(Long.MinValue, Long.MaxValue).toVector,
          Vector(
            HistoricEvent(timestamp + 2, timestamp + 2),
            HistoricEvent(timestamp + 1, timestamp + 1),
            HistoricEvent(timestamp, timestamp)

          )
        )
    }

  }

  test("add two vertices into partition, at t1 and t2 test window iterator") {

    Using(mkPartition(1, 0)) {
      par =>
        val t1 = System.currentTimeMillis()
        val t2 = t1 + 1

        // add bob
        addVertex(3, t1, None, ImmutableString("name", "Bob"))(par)
        // add alice
        addVertex(7, t2, None, ImmutableString("name", "Alice"))(par)

        par.flush
        val vs = par.vertices
        assertEquals(vs.size, 2)
        val vs1 = par.windowVertices(t1, t2) // inclusive window
        assertEquals(vs1.size, 2)
        val vs2 = par.windowVertices(t1, t1) // exclude t2
        assertEquals(vs2.size, 1)
        val vs3 = par.windowVertices(t2, t2 + 1) // exclude t2
        assertEquals(vs3.size, 1)

    }
  }

  test("edges with no properties should have .. not properties") {

    Using(mkPartition(1, 0)) {
      par =>
        val timestamp = System.currentTimeMillis()

        // add bob
        addVertex(3, timestamp, None, ImmutableString("name", "Bob"))(par)
        // add alice
        addVertex(7, timestamp, None, ImmutableString("name", "Alice"))(par)
        // add edge
        par.addLocalEdge(
          timestamp,
          -1,
          3,
          7,
          Properties(),
          None
        )

        par.flush()

        val edgeProps = par.vertices.flatMap { v =>
          v.outgoingEdges.flatten(e => e.prop[Long]("weight").list.take(2).toVector).toVector
        }.toVector

        assertEquals(edgeProps, Vector.empty)

    }

  }

  test("add edge between two vertices locally") {

    Using(mkPartition(1, 0)) { par =>
      val timestamp = System.currentTimeMillis()

      // add bob
      addVertex(3, timestamp, None, ImmutableString("name", "Bob"))(par)
      // add alice
      addVertex(7, timestamp, None, ImmutableString("name", "Alice"))(par)
      // add edge
      par.addLocalEdge(
              timestamp,
              -1,
              3,
              7,
              Properties(
                      ImmutableString("name", "friends"),
                      MutableLong("weight", 7)
              ),
              None
      )

      par.flush()
      val vs         = par.vertices.toList
      assertEquals(vs.size, 2)
      val names      = par.vertices.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name)).toSet
      assertEquals(names, Set(3L -> "Bob", 7L -> "Alice"))

      val neighbours = vs
        .flatMap {
          case v if v.field[String]("name").get.contains("Bob")   =>
            v.outgoingEdges.flatMap(e => e.field[String]("name").get.map(name => e.getDstVertex -> name)).toList
          case v if v.field[String]("name").get.contains("Alice") =>
            v.incomingEdges.flatMap(e => e.field[String]("name").get.map(name => e.getSrcVertex -> name)).toList
        }
        .sortBy(_._1)

      val expected = par.vertices.map(v => v.getLocalId -> "friends").toList.sortBy(_._1)
      assertEquals(neighbours, expected) // local ids are returned

      // add the edge again with a different time and different payload
      par.addLocalEdge(
              timestamp + 1,
              -1,
              3,
              7,
              Properties(
                      ImmutableString("name", "friends"),
                      MutableLong("weight", 9)
              ),
              None
      )
      par.flush()
      val edges =
        par.vertices.flatMap(v => v.outgoingEdges.flatMap(e => e.prop[Long]("weight").list.toList)).map(_._1).toSet
      assertEquals(edges, Set(7L, 9L))

    }
  }

  test("add edge between two vertices locally, 3 times and track history") {
    Using(mkPartition(1, 0)) { par =>
      val timestamp = System.currentTimeMillis()

      // add bob
      addVertex(3, timestamp, None, ImmutableString("name", "Bob"))(par)
      // add alice
      addVertex(7, timestamp, None, ImmutableString("name", "Alice"))(par)
      // add edge
      for (i <- 0 until 3)
        par.addLocalEdge(
          timestamp + i,
          -1,
          3,
          7,
          Properties(
            ImmutableString("name", "friends"),
            MutableLong("weight", 7)
          ),
          None
        )

      par.flush()

      val bobLocalId = par.localEntityStore.getLocalNodeId(3)
      val bob = par.getVertex(bobLocalId)
      val edge = bob.outgoingEdges.flatMap(e => e.history(timestamp, timestamp + 1).toVector).toVector

      assertEquals(
        edge,
        Vector(
          HistoricEvent(timestamp + 1, timestamp + 1),
          HistoricEvent(timestamp, timestamp)
        )
      )

      val edgeMAX = bob.outgoingEdges.flatMap(e => e.history(Long.MinValue, Long.MaxValue).toVector).toVector

      assertEquals(
        edgeMAX,
        Vector(
          HistoricEvent(timestamp + 2, timestamp + 2),
          HistoricEvent(timestamp + 1, timestamp + 1),
          HistoricEvent(timestamp, timestamp)
        )
      )
    }
  }

  test("add edge between the same vertex") {
    Using(mkPartition(1, 0)){
      par =>
        val timestamp = System.currentTimeMillis()

        // add bob
        addVertex(3, timestamp, None, ImmutableString("name", "Bob"))(par)
        // add alice
        addVertex(3, timestamp, None)(par)
        // add edge
        par.addLocalEdge(
          timestamp,
          -1,
          3,
          3,
          Properties(
            ImmutableString("name", "friends"),
            MutableLong("weight", 7)
          ),
          None
        )

        par.flush
        val bob = par.vertices.headOption.get
        assertEquals(bob.outgoingEdges.toVector.size, 1)
        val outEdges = bob.outgoingEdges(timestamp, timestamp + 1).map(e => e.getSrcVertex -> e.getDstVertex).toVector
        val inEdges = bob.incomingEdges(timestamp, timestamp + 1).map(e => e.getSrcVertex -> e.getDstVertex).toVector

        assertEquals(inEdges, Vector(0L -> 0L))
        assertEquals(outEdges, Vector(0L -> 0L))
    }
  }

  test("add edge between two vertices on separate partitions") {
    Using(mkPartition(2, 0)){ par1 =>
        Using(mkPartition(2, 1)){ par2 =>

          val timestamp = System.currentTimeMillis()
          val properties = Properties(ImmutableString("name", "friends"))

          // add bob
          addVertex(2, timestamp, None, ImmutableString("name", "Bob"))(par1)
          // add alice
          addVertex(7, timestamp, None, ImmutableString("name", "Alice"))(par2)
          // add edge on par1
          par1
            .addOutgoingEdge(
              timestamp,
              -1,
              2,
              7,
              properties,
              None
            )

          //add the second edge onto partition2
          par2.addIncomingEdge(timestamp, -1, 2, 7, properties, None)

          par1.flush
          par2.flush
          val (vs, names) = partitionVertices(par1)
          assertEquals(vs.size, 1)
          assertEquals(names, List(2L -> "Bob"))

          val (vs2, names2) = partitionVertices(par2)
          assertEquals(vs.size, 1)
          assertEquals(names2, List(7L -> "Alice"))

          val neighbours = allNeighbours(vs)

          assertEquals(neighbours, List((0L, 7L, "friends", false, true)))

          val neighbours2 = allNeighbours(vs2)

          assertEquals(
            neighbours2,
            List((2L, 0L, "friends", true, false))
          )

        }
    }


  }

  private def allNeighbours(vs: List[Vertex]) = {
    val neighbours = vs.flatMap {
      case v if v.field[String]("name").get.contains("Bob")   =>
        v.outgoingEdges.flatMap(e =>
          e.field[String]("name").get.map(name => (e.getSrcVertex, e.getDstVertex, name, e.isSrcGlobal, e.isDstGlobal))
        )
      case v if v.field[String]("name").get.contains("Alice") =>
        v.incomingEdges.flatMap(e =>
          e.field[String]("name").get.map(name => (e.getSrcVertex, e.getDstVertex, name, e.isSrcGlobal, e.isDstGlobal))
        )
    }
    neighbours
  }

  private def partitionVertices(par2: ArrowPartition) = {
    val vs2    = par2.vertices.toList
    val names2 = vs2.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name).toList)
    (vs2, names2)
  }

  private def mkPartition(nPartitions: Int, partitionId: Int, propSchema: PropertySchema = defaultPropSchema) = {
    val rConfig =
      ConfigBuilder()
        .addConfig("raphtory.partitions.countPerServer", s"$nPartitions")
        .addConfig("raphtory.partitions.serverCount", "1")
        .config

    val cfg = ArrowPartitionConfig(
            config = rConfig,
            partitionId = partitionId,
            propertySchema = propSchema,
            arrowDir = Files.createTempDirectory("arrow-storage-test"),
            Some(32),
            Some(32)
    )

    val par = ArrowPartition("test-graph-1", cfg, rConfig)
    par
  }

  private def addVertex(globalId: Long, timestamp: Long, tpe: Option[Type], props: Property*)(
      par: ArrowPartition
  ): Unit =
    par.addVertex(
            msgTime = timestamp,
            -1,
            srcId = globalId,
            Properties(props: _*),
            tpe
    )

  test("can generate Vertex schema from case class") {
    val actual  = VertexSchema.gen[VertexProp].nonVersionedVertexProps(None).map(f => f.name()).toList
    assertEquals(actual, List("name", "address_chain", "transaction_hash"))
    val actual2 = VertexSchema.gen[VertexProp].versionedVertexProps(None).map(f => f.name()).toList
    assertEquals(actual2, List("age", "weight"))
  }

  test("can generate Edge schema from case class") {
    val actual  = EdgeSchema.gen[EdgeProp].nonVersionedEdgeProps(None).map(f => f.name()).toList
    assertEquals(actual, List("name", "msgid", "subject"))
    val actual2 = EdgeSchema.gen[EdgeProp].versionedEdgeProps(None).map(f => f.name()).toList
    assertEquals(actual2, List("friends", "weight"))
  }

}

case class AllProps(
    @immutable name: String,
    pLong: Long,
    pDouble: Double,
    pFloat: Float,
    pBool: Boolean,
    pInt: Int,
    pString: String
)

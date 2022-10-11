package com.raphtory.storage

import com.raphtory.Raphtory
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.input
import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.arrowcore.model.PropertySchema
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.components.partition.EdgeProp
import com.raphtory.internals.components.partition.VertexProp
import com.raphtory.internals.graph.GraphAlteration.SyncNewEdgeAdd
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.LocalEntityRepo
import com.raphtory.internals.storage.arrow.RichEdge
import com.raphtory.internals.storage.arrow.RichVertex
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.arrow.immutable

import java.nio.file.Files

class ArrowStorageSuite extends munit.FunSuite {

  private val bobGlobalId   = 7L
  private val aliceGlobalId = 9L

  private val defaultPropSchema = ArrowSchema[VertexProp, EdgeProp]

  test("one can create a partition manager, add a vertex then get it back") {
    val cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig()
    cfg._propertySchema = ArrowSchema[VertexProp, EdgeProp]
    cfg._arrowDir = Files.createTempDirectory("arrow-storage-test").toString
    cfg._raphtoryPartitionId = 0
    cfg._nRaphtoryPartitions = 1
    cfg._nLocalEntityIdMaps = 128
    cfg._localEntityIdMapSize = 64
    cfg._syncIDMap = true

    val partition = new RaphtoryArrowPartition(cfg)

    val emgr      = partition.getEdgeMgr
    val vmgr      = partition.getVertexMgr
    val globalIds = partition.getGlobalEntityIdStore
    val localIds  = partition.getLocalEntityIdStore

    val partitionId = vmgr.getNewPartitionId

    assertEquals(partitionId, 0)

    val localId = vmgr.getNextFreeVertexId

    assertEquals(localId, 0L)

    val NAME_FIELD_ID = partition.getVertexFieldId("name")
    val AGE_FIELD_ID  = partition.getVertexPropertyId("age")

    var bobV = vmgr.getVertex(localId)

    assert(bobV == null)
    bobV = partition.getVertex
    bobV.reset(localId, bobGlobalId, true, System.currentTimeMillis())
    bobV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    bobV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)

    vmgr.addVertex(bobV)
    assertEquals(localIds.getLocalNodeId(bobGlobalId), 0L)

    val vs: VertexIterator.AllVerticesIterator = partition.getNewAllVerticesIterator
    vs.hasNext // this has to be called before getVertex
    val v2 = vs.getVertex

    assertEquals(v2.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(v2.getProperty(AGE_FIELD_ID).getLong, 45L)

  }

  test("add Bob and Alice and make an edge") {
    val cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig()
    cfg._propertySchema = ArrowSchema[VertexProp, EdgeProp]
    cfg._arrowDir = Files.createTempDirectory("arrow-storage-test").toString
    cfg._raphtoryPartitionId = 0
    cfg._nRaphtoryPartitions = 1
    cfg._nLocalEntityIdMaps = 128
    cfg._localEntityIdMapSize = 64
    cfg._vertexPartitionSize = 128
    cfg._syncIDMap = true

    val partition = new RaphtoryArrowPartition(cfg)

    val emgr                         = partition.getEdgeMgr
    val vmgr                         = partition.getVertexMgr
    val globalIds                    = partition.getGlobalEntityIdStore
    val localIds: LocalEntityIdStore = partition.getLocalEntityIdStore
    val idsRepo                      = new LocalEntityRepo(localIds, 1, 0)

    val partitionId = vmgr.getNewPartitionId

    assertEquals(partitionId, 0)

    val NAME_FIELD_ID = partition.getVertexFieldId("name")
    val AGE_FIELD_ID  = partition.getVertexPropertyId("age")

    val localId = vmgr.getNextFreeVertexId
    assertEquals(localId, 0L)
    var bobV    = vmgr.getVertex(localId)

    assert(bobV == null)
    bobV = partition.getVertex
    bobV.reset(localId, bobGlobalId, true, System.currentTimeMillis())
    bobV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    bobV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)
    vmgr.addVertex(bobV)
    assertEquals(localIds.getLocalNodeId(bobGlobalId), 0L)

    val localId2 = vmgr.getNextFreeVertexId
    assertEquals(localId2, 1L)
    var aliceV   = vmgr.getVertex(localId2)

    assert(aliceV == null)
    aliceV = partition.getVertex
    aliceV.reset(localId2, aliceGlobalId, true, System.currentTimeMillis())
    aliceV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Alice"))
    aliceV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(47L)
    vmgr.addVertex(aliceV)
    assertEquals(localIds.getLocalNodeId(bobGlobalId), localId)
    assertEquals(localIds.getLocalNodeId(aliceGlobalId), localId2)

    val vs: VertexIterator.AllVerticesIterator = partition.getNewAllVerticesIterator
    vs.hasNext // this has to be called before getVertex
    vs.next()
    val bob: Vertex = vs.getVertex

    assertEquals(bob.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(bob.getProperty(AGE_FIELD_ID).getLong, 45L)

    vs.hasNext // this has to be called before getVertex
    vs.next()
    val alice = vs.getVertex
    assertEquals(alice.getField(NAME_FIELD_ID).getString.toString, "Alice")
    assertEquals(alice.getProperty(AGE_FIELD_ID).getLong, 47L)

    // add an edge between bob and alice
    val timestamp = System.currentTimeMillis()
    val eId       = emgr.getNextFreeEdgeId
    val e         = partition.getEdge
    e.init(eId, true, timestamp)
    // figure out if the src node is local or global
    val src       = idsRepo.resolve(bobGlobalId)
    // figure out if the dst node is local or global
    val dst       = idsRepo.resolve(aliceGlobalId)
    e.resetEdgeData(src.id, dst.id, -1L, -1L, src.isGlobal, dst.isGlobal)
    emgr.addEdge(e, -1L, -1L)
    emgr.addHistory(e.getLocalId, timestamp, true, false)

    if (src.isLocal) {
      val p = vmgr.getPartition(vmgr.getPartitionId(src.id))

      val prevListPtr = p.synchronized {
        val ptr = p.addOutgoingEdgeToList(src.id, e.getLocalId, dst.id)
        p.addHistory(src.id, timestamp, true, false, e.getLocalId, true)
        ptr
      }

      emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)

    }

    if (dst.isLocal) {
      val p           = vmgr.getPartition(vmgr.getPartitionId(dst.id))
      val prevListPtr = p.synchronized {
        val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
        p.addHistory(dst.id, timestamp, true, false, e.getLocalId, false)
        ptr
      }

      emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
    }

    // we've got 2 nodes connected by an edge let's put that to good use

    assertEquals(emgr.getTotalNumberOfEdges, 1L)

    val eIter      = partition.getNewAllEdgesIterator
    assert(eIter.hasNext)
    val actualEdge = eIter.getEdge
    assert(actualEdge != null)

    assertEquals(actualEdge.getSrcVertex, bob.getLocalId)
    assertEquals(actualEdge.getDstVertex, alice.getLocalId)

    val vs2: VertexIterator.AllVerticesIterator = partition.getNewAllVerticesIterator
    // this should be bob
    vs2.hasNext
    vs2.next()
    val v1                                      = vs2.getVertex
    assertEquals(v1.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(v1.nOutgoingEdges(), 1)
    assertEquals(v1.nIncomingEdges(), 0)

    // bob's edges
    val bobIterE = v1.getOutgoingEdges

    assert(bobIterE.hasNext)
    bobIterE.next()
    val bobOutE = bobIterE.getEdge

    assert(bobOutE != null)
    assert(!bobIterE.hasNext) // idempotent
    assertEquals(bobOutE.getDstVertex, alice.getLocalId)
    assertEquals(bobOutE.getSrcVertex, bob.getLocalId)
    assertEquals(bobOutE.isDstGlobal, false)

    // this should be alice
    vs2.hasNext
    vs2.next()
    val v2 = vs2.getVertex

    assertEquals(v2.getField(NAME_FIELD_ID).getString.toString, "Alice")
    assertEquals(v2.nOutgoingEdges(), 0)
    assertEquals(v2.nIncomingEdges(), 1)

    val aliceIterE = v2.getIncomingEdges
    assert(aliceIterE.hasNext)

    aliceIterE.next()
    val aliceInE = aliceIterE.getEdge
    assert(aliceInE != null)
    assert(!aliceIterE.hasNext)
  }

  test("add vertex with all types of properties and vertex type") {
    val par: ArrowPartition = mkPartition(1, 0, ArrowSchema[AllProps, EdgeProp])
    val timestamp           = System.currentTimeMillis()

    // add bob
    val now = System.currentTimeMillis()
    addVertex(
            3,
            timestamp,
            Some(Type("Person")),
            LongProperty("pLong", now),
            DoubleProperty("pDouble", 1.234d),
            FloatProperty("pFloat", 4.321f),
            BooleanProperty("pBool", value = true),
            StringProperty("pString", "blerg"),
            IntegerProperty("pInt", 12345789),
            ImmutableProperty("name", "Bob")
    )(par)

    val actual = par.vertices.head

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
            LongProperty("pLong", now + 1)
    )(par)

    val actual2 = par.vertices.head
    assertEquals(actual2.prop[Long]("pLong").list.toSet, Set(now -> timestamp, (now + 1, timestamp + 1)))
  }

  test("add two vertices into partition") {

    val par: ArrowPartition = mkPartition(1, 0)
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, None, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, None, ImmutableProperty("name", "Alice"))(par)

    val vs = par.vertices
    assertEquals(vs.size, 2)

    val names = vs.iterator.toList.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))

    assertEquals(names, List(3L -> "Bob", 7L -> "Alice"))

  }

  test("add a vertex 3 times and explore the history") {

    val par: ArrowPartition = mkPartition(1, 0)
    val timestamp           = System.currentTimeMillis()

    // add bob at t1
    val property = ImmutableProperty("name", "Bob")
    addVertex(3, timestamp, None, property)(par)
    // add bob at t3
    addVertex(3, timestamp + 2, None, property)(par)
    // add bob at t2
    addVertex(3, timestamp + 1, None, property)(par)

    val vs = par.vertices
    assertEquals(vs.size, 1)

    val names = vs.iterator.toList.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))

    assertEquals(names, List(3L -> "Bob"))

    val bob = par.vertices.head

    assertEquals(
            bob.history(Long.MinValue, Long.MaxValue).toVector,
            Vector(
                    HistoricEvent(timestamp + 2, timestamp + 2),
                    HistoricEvent(timestamp + 1, timestamp + 1),
                    HistoricEvent(timestamp, timestamp)
            )
    )

  }

  test("add two vertices into partition, at t1 and t2 test window iterator") {

    val par: ArrowPartition = mkPartition(1, 0)
    val t1                  = System.currentTimeMillis()
    val t2                  = t1 + 1

    // add bob
    addVertex(3, t1, None, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, t2, None, ImmutableProperty("name", "Alice"))(par)

    val vs  = par.vertices
    assertEquals(vs.size, 2)
    val vs1 = par.windowVertices(t1, t2)     // inclusive window
    assertEquals(vs1.size, 2)
    val vs2 = par.windowVertices(t1, t1)     // exclude t2
    assertEquals(vs2.size, 1)
    val vs3 = par.windowVertices(t2, t2 + 1) // exclude t2
    assertEquals(vs3.size, 1)

  }

  test("add edge between two vertices locally") {

    val par: ArrowPartition = mkPartition(1, 0)
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, None, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, None, ImmutableProperty("name", "Alice"))(par)
    // add edge
    val action = par.addEdge(
            3,
            timestamp,
            -1,
            3,
            7,
            Properties(
                    ImmutableProperty("name", "friends"),
                    LongProperty("weight", 7)
            ),
            None
    )
    // nothing to do both nodes are on the same partition
    assert(action.isEmpty)

    val vs = par.vertices.toList
    assertEquals(vs.size, 2)

    val names      = vs.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))
    assertEquals(names, List(3L -> "Bob", 7L -> "Alice"))

    val neighbours = vs.flatMap {
      case v if v.field[String]("name").get.contains("Bob")   =>
        v.outgoingEdges.flatMap(e => e.field[String]("name").get.map(name => e.getDstVertex -> name))
      case v if v.field[String]("name").get.contains("Alice") =>
        v.incomingEdges.flatMap(e => e.field[String]("name").get.map(name => e.getSrcVertex -> name))
    }

    assertEquals(neighbours, List(1L -> "friends", 0L -> "friends")) // local ids are returned

    // add the edge again with a different time and different payload
    par.addEdge(
            3,
            timestamp + 1,
            -1,
            3,
            7,
            Properties(
                    ImmutableProperty("name", "friends"),
                    LongProperty("weight", 9)
            ),
            None
    )

    val edges = par.vertices.flatMap(v => v.outgoingEdges.flatMap(e => e.prop[Long]("weight").list)).map(_._1).toSet
    assertEquals(edges, Set(7L, 9L))

  }

  test("add edge between two vertices locally, 3 times and track history") {

    val par: ArrowPartition = mkPartition(1, 0)
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, None, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, None, ImmutableProperty("name", "Alice"))(par)
    // add edge
    for (i <- 0 until 3)
      par.addEdge(
              3,
              timestamp + i,
              -1,
              3,
              7,
              Properties(
                      ImmutableProperty("name", "friends"),
                      LongProperty("weight", 7)
              ),
              None
      )

    val bob  = par.vertices.head
    val edge = bob.outgoingEdges.head
    assertEquals(
            edge.history(timestamp, timestamp + 1).toVector,
            Vector(
                    HistoricEvent(timestamp + 1, timestamp + 1),
                    HistoricEvent(timestamp, timestamp)
            )
    )
    assertEquals(
            edge.history(Long.MinValue, Long.MaxValue).toVector,
            Vector(
                    HistoricEvent(timestamp + 2, timestamp + 2),
                    HistoricEvent(timestamp + 1, timestamp + 1),
                    HistoricEvent(timestamp, timestamp)
            )
    )

    test("add edge between the same vertex".only) {

      val par: ArrowPartition = mkPartition(1, 0)
      val timestamp           = System.currentTimeMillis()

      // add bob
      addVertex(3, timestamp, None, ImmutableProperty("name", "Bob"))(par)
      // add alice
      addVertex(3, timestamp, None)(par)
      // add edge
      par.addEdge(
              3,
              timestamp,
              -1,
              3,
              3,
              Properties(
                      ImmutableProperty("name", "friends"),
                      LongProperty("weight", 7)
              ),
              None
      )

      val bob = par.vertices.head
      assertEquals(bob.outgoingEdges.toVector.size, 1)
      val outEdges = bob.outgoingEdges(timestamp, timestamp+1).map(e => e.getSrcVertex -> e.getDstVertex).toVector
      val inEdges = bob.incomingEdges(timestamp, timestamp+1).map(e => e.getSrcVertex -> e.getDstVertex).toVector

      assertEquals(outEdges, inEdges)
    }

    // now remove it
    par.removeEdge(3, timestamp + 3, -1, 3, 7)

    assertEquals(
            edge.history(Long.MinValue, Long.MaxValue).toVector,
            Vector(
                    HistoricEvent(timestamp + 3, timestamp + 3, event = false),
                    HistoricEvent(timestamp + 2, timestamp + 2),
                    HistoricEvent(timestamp + 1, timestamp + 1),
                    HistoricEvent(timestamp, timestamp)
            )
    )

  }

  test("removed edge should be visible depending on iterator time window".ignore) {

    val par: ArrowPartition = mkPartition(1, 0)
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, None, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, None, ImmutableProperty("name", "Alice"))(par)
    // add edge
    val action = par.addEdge(
            3,
            timestamp,
            -1,
            3,
            7,
            Properties(
                    ImmutableProperty("name", "friends")
            ),
            None
    )

    par.removeEdge(3, timestamp + 1, -1, 3, 7)
  }

  test("add edge between two vertices on separate partitions") {

    val par1: ArrowPartition = mkPartition(2, 0)
    val par2: ArrowPartition = mkPartition(2, 1)
    val timestamp            = System.currentTimeMillis()

    // add bob
    addVertex(2, timestamp, None, ImmutableProperty("name", "Bob"))(par1)
    // add alice
    addVertex(7, timestamp, None, ImmutableProperty("name", "Alice"))(par2)
    // add edge on par1
    val action                                                                        = par1
      .addEdge(
              3,
              timestamp,
              -1,
              2,
              7,
              Properties(
                      ImmutableProperty("name", "friends")
              ),
              None
      )
      .collect { case a: SyncNewEdgeAdd => a }

    // nothing to do both nodes are on the same partition
    assert(action.isDefined)
    val SyncNewEdgeAdd(sourceID, updateTime, index, srcId, dstId, properties, _, tpe) = action.get

    //add the second edge onto partition2
    par2.syncNewEdgeAdd(sourceID, updateTime, index, srcId, dstId, properties, List.empty, tpe)

    val (vs, names)   = partitionVertices(par1)
    assertEquals(vs.size, 1)
    assertEquals(names, List(2L -> "Bob"))

    val (vs2, names2) = partitionVertices(par2)
    assertEquals(vs.size, 1)
    assertEquals(names2, List(7L -> "Alice"))

    val neighbours    = allNeighbours(vs)

    assertEquals(neighbours, List((0L, 7L, "friends", false, true)))

    val neighbours2 = allNeighbours(vs2)

    assertEquals(
            neighbours2,
            List((2L, 0L, "friends", true, false))
    )

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
    val names2 = vs2.flatMap(v => v.field[String]("name").get.map(name => v.getGlobalId -> name))
    (vs2, names2)
  }

  private def mkPartition(nPartitions: Int, partitionId: Int, propSchema: PropertySchema = defaultPropSchema) = {
    val rConfig =
      Raphtory.getDefaultConfig(
              Map("raphtory.partitions.serverCount" -> 1, "raphtory.partitions.countPerServer" -> nPartitions)
      )

    val cfg = ArrowPartitionConfig(
            config = rConfig,
            partitionId = partitionId,
            propertySchema = propSchema,
            arrowDir = Files.createTempDirectory("arrow-storage-test")
    )

    val par = ArrowPartition("test-graph-1", cfg, rConfig)
    par
  }

  private def addVertex(globalId: Long, timestamp: Long, tpe: Option[Type], props: Property*)(
      par: ArrowPartition
  ): Unit =
    par.addVertex(
            sourceID = 3,
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
    assertEquals(actual2, List("age"))
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

//case class NodeSchema(@immutable name: String)
//case class EdgeSchema(weight: Long, @immutable msgId: String, @immutable subject: String)

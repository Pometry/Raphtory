package com.raphtory.storage

import com.raphtory.Raphtory
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Property
import com.raphtory.api.input.Properties
import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.storage.arrow
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.LocalEntityRepo
import com.raphtory.internals.storage.arrow.RichVertex
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.arrow.versioned

import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneOffset

class ArrowStorageSuite extends munit.FunSuite {

  private val bobGlobalId   = 7L
  private val aliceGlobalId = 9L
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
    val bob: Vertex = vs.getVertex

    val richBob = arrow.RichVertex[VertexProp, EdgeProp](bob)

    assertEquals(bob.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(bob.getProperty(AGE_FIELD_ID).getLong, 45L)

    vs.hasNext // this has to be called before getVertex
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
    emgr.addHistory(e.getLocalId, timestamp, true)

    if (src.isLocal) {
      val p = vmgr.getPartition(vmgr.getPartitionId(src.id))

      val prevListPtr = p.synchronized {
        val ptr = p.addOutgoingEdgeToList(src.id, e.getLocalId)
        p.addHistory(src.id, timestamp, true, e.getLocalId, true)
        ptr
      }

      emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)

    }

    if (dst.isLocal) {
      val p           = vmgr.getPartition(vmgr.getPartitionId(dst.id))
      val prevListPtr = p.synchronized {
        val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
        p.addHistory(dst.id, timestamp, true, e.getLocalId, false)
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
    val v1                                      = vs2.getVertex
    assertEquals(v1.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(v1.nOutgoingEdges(), 1)
    assertEquals(v1.nIncomingEdges(), 0)

    // bob's edges
    val bobIterE = partition.getNewAllEdgesIterator
    bobIterE.reset(v1.getOutgoingEdgePtr)
    assert(bobIterE.hasNext)
    val bobOutE  = bobIterE.getEdge
    assert(bobOutE != null)
    assert(!bobIterE.hasNext) // idempotent
    assertEquals(bobOutE.getDstVertex, alice.getLocalId)
    assertEquals(bobOutE.getSrcVertex, bob.getLocalId)
    assertEquals(bobOutE.isDstGlobal, false)

    // this should be alice
    vs2.hasNext
    val v2 = vs2.getVertex

    assertEquals(v2.getField(NAME_FIELD_ID).getString.toString, "Alice")
    assertEquals(v2.nOutgoingEdges(), 0)
    assertEquals(v2.nIncomingEdges(), 1)

  }

  test("add two vertices into partition".only) {

    val par: ArrowPartition = mkPartition
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, ImmutableProperty("name", "Alice"))(par)

    val vs = par.vertices
    assertEquals(vs.size, 2)

    val names = vs.iterator.toList.map(v => v.getGlobalId -> v.prop[String]("name").get)

    assertEquals(names, List(3L -> "Bob", 7L -> "Alice"))

  }

  test("add edge between two vertices".only) {

    val par: ArrowPartition = mkPartition
    val timestamp           = System.currentTimeMillis()

    // add bob
    addVertex(3, timestamp, ImmutableProperty("name", "Bob"))(par)
    // add alice
    addVertex(7, timestamp, ImmutableProperty("name", "Alice"))(par)
    // add edge

    par.addEdge(3, timestamp, -1, 3, 7, Properties(), None)

    val vs = par.vertices.toList
    assertEquals(vs.size, 2)

    val names                  = vs.map(v => v.getGlobalId -> v.prop[String]("name").get)
    assertEquals(names, List(3L -> "Bob", 7L -> "Alice"))

    val neighbours: List[Long] = vs.collect {
      case v if v.prop[String]("name").get == "Bob"   =>
        v.outgoingEdges.map(_.getDstVertex).toList // FIXME: without toList this fails
      case v if v.prop[String]("name").get == "Alice" =>
        v.incomingEdges.map(_.getSrcVertex).toList // FIXME: without toList this fails
    }.flatten

    assertEquals(neighbours, List(1L, 0L)) // local ids are returned

  }

  private def mkPartition = {
    val rConfig =
      Raphtory.getDefaultConfig(Map("raphtory.partitions.serverCount" -> 1, "raphtory.partitions.countPerServer" -> 1))

    val cfg     = ArrowPartitionConfig(
            config = rConfig,
            partitionId = 0,
            propertySchema = ArrowSchema[VertexProp, EdgeProp],
            arrowDir = Files.createTempDirectory("arrow-storage-test")
    )

    val par = ArrowPartition(cfg, rConfig)
    par
  }

  private def addVertex(globalId: Long, timestamp: Long, props: Property*)(par: ArrowPartition): Unit =
    par.addVertex(
            sourceID = 3,
            msgTime = timestamp,
            -1,
            srcId = globalId,
            Properties(props: _*),
            None
    )

  test("can generate Vertex schema from case class") {
    val actual  = VertexSchema.gen[VertexProp].nonVersionedVertexProps(None).map(f => f.name()).toList
    assertEquals(actual, List("name"))
    val actual2 = VertexSchema.gen[VertexProp].versionedVertexProps(None).map(f => f.name()).toList
    assertEquals(actual2, List("age"))
  }

  test("can generate Edge schema from case class") {
    val actual  = EdgeSchema.gen[EdgeProp].nonVersionedEdgeProps(None).map(f => f.name()).toList
    assertEquals(actual, List("name"))
    val actual2 = EdgeSchema.gen[EdgeProp].versionedEdgeProps(None).map(f => f.name()).toList
    assertEquals(actual2, List("friends"))
  }

}

case class VertexProp(@versioned age: Long, name: String)
case class EdgeProp(name: String, @versioned friends: Boolean)

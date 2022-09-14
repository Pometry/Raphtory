package com.raphtory.storage

import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.LocalEntityRepo
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.arrow.versioned

import java.nio.file.Files

class ArrowStorageSuite extends munit.FunSuite {

  private val bobGlobalId = 7L

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

  val aliceGlobalId = 9L
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
    val idsRepo                      = new LocalEntityRepo(localIds)

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
    val bob = vs.getVertex

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
      val p = vmgr.getPartition(vmgr.getPartitionId(dst.id))
      val prevListPtr = p.synchronized{
        val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
        p.addHistory(dst.id, timestamp, true, e.getLocalId, false)
        ptr
      }

      emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
    }

    // we've got 2 nodes connected by an edge let's put that to good use

    assertEquals(emgr.getTotalNumberOfEdges, 1L)

    val eIter = partition.getNewAllEdgesIterator
    assert(eIter.hasNext)
    val actualEdge = eIter.getEdge
    assert(actualEdge != null)

    assertEquals(actualEdge.getSrcVertex, bob.getLocalId)
    assertEquals(actualEdge.getDstVertex, alice.getLocalId)

    val vs2: VertexIterator.AllVerticesIterator = partition.getNewAllVerticesIterator
    // this should be bob
    vs2.hasNext
    val v1 = vs2.getVertex
    assertEquals(v1.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(v1.getIncomingEdges.size(), 0)
    assertEquals(v1.getOutgoingEdges.size(), 1)

    // this should be alice
    vs2.hasNext
    val v2 = vs2.getVertex

    assertEquals(v2.getField(NAME_FIELD_ID).getString.toString, "Alice")
    assertEquals(v2.getIncomingEdges.size(), 1)
    assertEquals(v2.getOutgoingEdges.size(), 0)

  }

  test("I want to iterate over all vertices") {

    val cfg = ArrowPartitionConfig(
            partitionId = 0,
            nPartitions = 1,
            propertySchema = ArrowSchema[VertexProp, EdgeProp],
            arrowDir = Files.createTempDirectory("arrow-storage-test")
    )

    val par = ArrowPartition(cfg)

//    par.addVertex(msgTime, nodeId, properties, None)
  }

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

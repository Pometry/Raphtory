package com.raphtory.storage

import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexIterator
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.internals.storage.arrow.versioned

import java.nio.file.Files

class ArrowStorageSuite extends munit.FunSuite {

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
    bobV.reset(localId, 7L, true, System.currentTimeMillis())
    bobV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    bobV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)
    vmgr.addVertex(bobV)
    assertEquals(localIds.getLocalNodeId(7L), 0L)

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

    val emgr      = partition.getEdgeMgr
    val vmgr      = partition.getVertexMgr
    val globalIds = partition.getGlobalEntityIdStore
    val localIds  = partition.getLocalEntityIdStore

    val partitionId = vmgr.getNewPartitionId

    assertEquals(partitionId, 0)

    val NAME_FIELD_ID = partition.getVertexFieldId("name")
    val AGE_FIELD_ID  = partition.getVertexPropertyId("age")

    val localId = vmgr.getNextFreeVertexId
    assertEquals(localId, 0L)
    var bobV    = vmgr.getVertex(localId)

    assert(bobV == null)
    bobV = partition.getVertex
    bobV.reset(localId, 7L, true, System.currentTimeMillis())
    bobV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    bobV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)
    vmgr.addVertex(bobV)
    assertEquals(localIds.getLocalNodeId(7L), 0L)

    val localId2 = vmgr.getNextFreeVertexId
    assertEquals(localId2, 1L)
    var aliceV   = vmgr.getVertex(localId2)

    assert(aliceV == null)
    aliceV = partition.getVertex
    aliceV.reset(localId2, 9L, true, System.currentTimeMillis())
    aliceV.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Alice"))
    aliceV.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(47L)
    vmgr.addVertex(aliceV)
    assertEquals(localIds.getLocalNodeId(7L), localId)
    assertEquals(localIds.getLocalNodeId(9L), localId2)

    val vs: VertexIterator.AllVerticesIterator = partition.getNewAllVerticesIterator
    vs.hasNext // this has to be called before getVertex
    val bob = vs.getVertex

    assertEquals(bob.getField(NAME_FIELD_ID).getString.toString, "Bob")
    assertEquals(bob.getProperty(AGE_FIELD_ID).getLong, 45L)

    vs.hasNext // this has to be called before getVertex
    val alice = vs.getVertex
    assertEquals(alice.getField(NAME_FIELD_ID).getString.toString, "Alice")
    assertEquals(alice.getProperty(AGE_FIELD_ID).getLong, 47L)

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

package com.raphtory.storage

import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.internals.storage.arrow.{ArrowSchema, EdgeSchema, VertexSchema, versioned}

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

    val par = new RaphtoryArrowPartition(cfg)

    val emgr      = par.getEdgeMgr
    val vmgr      = par.getVertexMgr
    val globalIds = par.getGlobalEntityIdStore
    val localIds  = par.getLocalEntityIdStore

    val partitionId = vmgr.getNewPartitionId

    assertEquals(partitionId, 0)

    val firstLocalId = partitionId.toLong * vmgr.PARTITION_SIZE

    assertEquals(firstLocalId, 0L)

    var localVId: Long = firstLocalId

    var v = vmgr.getVertex(localVId)

    assert(v == null)

    v = par.getVertex
    v.incRefCount()

    val NAME_FIELD_ID = par.getVertexFieldId("name")
    val AGE_FIELD_ID  = par.getVertexPropertyId("age")

    v.reset(localVId, 7L, true, System.currentTimeMillis())
    v.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    v.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)

    vmgr.addVertex(v)
    localIds.setMapping(7L, 0L)

    assertEquals(localIds.getLocalNodeId(7L), 0L)
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

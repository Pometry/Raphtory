package com.raphtory.storage

import com.raphtory.arrowcore.implementation.NonversionedField
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VersionedProperty
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.PropertySchema

import java.nio.file.Files
import java.lang
import java.util
import scala.collection.mutable

class ArrowStorageSuite extends munit.FunSuite {

  test("one can create a partition manager, add a vertex then get it back") {
    val cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig()
    cfg._propertySchema = new TestSchema()
    cfg._arrowDir = Files.createTempDirectory("arrow-storage-test").toString
    cfg._raphtoryPartitionId = 0
    cfg._nRaphtoryPartitions = 1
    cfg._nLocalEntityIdMaps = 128
    cfg._localEntityIdMapSize = 64
    cfg._syncIDMap = true


    val par = new RaphtoryArrowPartition(cfg)

    val emgr = par.getEdgeMgr
    val vmgr = par.getVertexMgr
    val globalIds = par.getGlobalEntityIdStore
    val localIds = par.getLocalEntityIdStore

    val partitionId = vmgr.getNewPartitionId

    assertEquals(partitionId, 0)

    val firstLocalId = partitionId * vmgr.PARTITION_SIZE

    assertEquals(firstLocalId, 0)

    var localVId = firstLocalId

    var v = vmgr.getVertex(localVId)

    assert(v == null)

    v = par.getVertex
    v.incRefCount()

    val NAME_FIELD_ID = par.getVertexFieldId("name")
    val AGE_FIELD_ID = par.getVertexPropertyId("age")

    v.reset(localVId, 7, true, System.currentTimeMillis())
    v.getField(NAME_FIELD_ID).set(new java.lang.StringBuilder("Bob"))
    v.getProperty(AGE_FIELD_ID).setHistory(true, System.currentTimeMillis()).set(45L)

    vmgr.addVertex(v)
  }

}

class TestSchema extends PropertySchema {

  override val nonversionedVertexProperties: util.ArrayList[NonversionedField] =
    new util.ArrayList(
            util.Arrays.asList(
                    new NonversionedField("name", classOf[java.lang.StringBuilder])
            )
    )

  override val versionedVertexProperties: util.ArrayList[VersionedProperty] =
    new util.ArrayList(
            util.Arrays.asList(
                    new VersionedProperty("age", classOf[Long])
            )
    )

  override val nonversionedEdgeProperties: util.ArrayList[NonversionedField] = new util.ArrayList(
          util.Arrays.asList(
                  new NonversionedField("name", classOf[java.lang.StringBuilder])
          )
  )

  override val versionedEdgeProperties: util.ArrayList[VersionedProperty] = new util.ArrayList[VersionedProperty](
          util.Arrays.asList(
                  new VersionedProperty("friends", classOf[Boolean])
          )
  )
}

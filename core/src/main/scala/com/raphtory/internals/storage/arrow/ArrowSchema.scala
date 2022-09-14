package com.raphtory.internals.storage.arrow

import java.util
import com.raphtory.arrowcore.implementation.NonversionedField
import com.raphtory.arrowcore.implementation.VersionedProperty
import com.raphtory.arrowcore.model.PropertySchema

object ArrowSchema {
  import scala.jdk.CollectionConverters._

  case class ScalaPropertySchema(
      nonversionedVertexProperties: util.ArrayList[NonversionedField],
      versionedVertexProperties: util.ArrayList[VersionedProperty],
      nonversionedEdgeProperties: util.ArrayList[NonversionedField],
      versionedEdgeProperties: util.ArrayList[VersionedProperty]
  ) extends PropertySchema {

  }

  def apply[V, E](implicit V: VertexSchema[V], E: EdgeSchema[E]): PropertySchema =
    ScalaPropertySchema(
            new util.ArrayList(V.nonVersionedVertexProps(None).asJavaCollection),
            new util.ArrayList(V.versionedVertexProps(None).asJavaCollection),
            new util.ArrayList(E.nonVersionedEdgeProps(None).asJavaCollection),
            new util.ArrayList(E.versionedEdgeProps(None).asJavaCollection)
    )
}

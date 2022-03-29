package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

/**
  * {s}`TemporalNodeList(properties: String*) = new TemporalNodeList(properties)`
  *  : Write out timestamped nodes with selected properties to table
  *
  * {s}`TemporalNodeLisst(defaults: Map[String, Any], properties: String*)`
  *  : Specify default values for missing properties
  *
  * {s}`TemporalNodeList(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
  *  : Specify property names as sequence
  *
  * Property names are looked up on a node's computational state first and then in a node's property. Property values
  * are looked up using creation timestamps explicitly. Computational state is expanded if it is a sequence
  * with the same size as the vertex's distinct creation timestamps and assuming that state maps to a vertex's
  * distinct, sorted timestamps. This means that this algorithm can be used as the last step in an algorithm
  * chain to include output of intermediate results.
  *
  * ## Params
  *
  *  {s}`properties: Seq[String]`
  *    : Sequence of property names
  *
  *  {s}`defaults: Map[String, Any]`
  *    : Map from property names to default values (if a property name is not found on a node,
  *      it is first looked up in defaults, and if not found there, set to {s}`None`)
  *
  * ## Returns
  *
  *  | vertex name       | time stamp      | property 1      | ... |
  *  | ----------------- | --------------- | --------------- | --- |
  *  | {s}`name: String` | {s}`time: Long` | {s}`value: Any` | ... |
  */
class TemporalNodeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends GraphAlgorithm {

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select { vertex =>
        val timestamps =
          vertex.history().filter(event => event.event).map(event => event.time).distinct.sorted
        val row        = (vertex.name()
          +: timestamps
          +: properties.map { name =>
            vertex.getStateOrElse(
                    name,
                    timestamps.map(time =>
                      vertex.getPropertyAt[Any](name, time) match {
                        case Some(value) => value
                        case None        => defaults.getOrElse(name, None)
                      }
                    )
            )
          })
        Row(row: _*)
      }
      .explode { row =>
        val timestamps = row.getAs[List[Long]](1)

        (for ((time, index) <- row.getAs[List[Long]](1).view.zipWithIndex) yield {
          val expandedFields = for (fieldIndex <- properties.indices) yield {
            val fieldData = row(fieldIndex + 2)
            try {
              val fieldDataSeq = fieldData.asInstanceOf[Seq[Any]]
              if (fieldDataSeq.size == timestamps.size)
                fieldDataSeq(index)
              else
                fieldData
            }
            catch {
              case _: Throwable => fieldData
            }
          }
          Row(row(0) +: time +: expandedFields: _*)
        }).toList
      }
}

object TemporalNodeList {

  def apply(
      properties: Seq[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  )                              = new TemporalNodeList(properties, defaults)
  def apply(properties: String*) = new TemporalNodeList(properties)

  def apply(defaults: Map[String, Any], properties: String*) =
    new TemporalNodeList(properties, defaults)
}

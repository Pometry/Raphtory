package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor.ExplodedVertex
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.Vertex

/**
  * @note DoNotDocument
  */
//class GenericGraphPerspective(query: Query, private val querySender: QuerySender)
//        extends DefaultGraphOperations[
//                Vertex,
//                GenericGraphPerspective,
//                GenericGraphPerspective,
//                GenericMultilayerGraphPerspective
//        ](query, querySender)
//        with GraphPerspective[GenericGraphPerspective] {
//
//  override protected def newGraph(query: Query, querySender: QuerySender) =
//    new GenericGraphPerspective(query, querySender)
//
//  override protected def newRGraph(
//      query: Query,
//      querySender: QuerySender
//  ): GenericGraphPerspective = newGraph(query, querySender)
//
//  override protected def newMGraph(
//      query: Query,
//      querySender: QuerySender
//  ): GenericMultilayerGraphPerspective =
//    new GenericMultilayerGraphPerspective(query, querySender)
//}
//
//class GenericMultilayerGraphPerspective(query: Query, private val querySender: QuerySender)
//        extends DefaultGraphOperations[
//                ExplodedVertex,
//                GenericMultilayerGraphPerspective,
//                GenericGraphPerspective,
//                GenericMultilayerGraphPerspective
//        ](
//                query,
//                querySender
//        )
//        with GraphPerspective[GenericMultilayerGraphPerspective] {
//
//  override protected def newGraph(query: Query, querySender: QuerySender) =
//    new GenericMultilayerGraphPerspective(query, querySender)
//
//  override protected def newRGraph(
//      query: Query,
//      querySender: QuerySender
//  ): GenericGraphPerspective =
//    new GenericGraphPerspective(query, querySender)
//
//  override protected def newMGraph(
//      query: Query,
//      querySender: QuerySender
//  ): GenericMultilayerGraphPerspective =
//    newGraph(query, querySender)
//}

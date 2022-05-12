package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query

/**
  * @note DoNotDocument
  */
class GenericGraphPerspective(query: Query, private val querySender: QuerySender)
        extends DefaultGraphOperations[GraphPerspective](query, querySender)
        with GraphPerspective {

  override protected def newGraph(query: Query, querySender: QuerySender) =
    new GenericGraphPerspective(query, querySender)
}

package com.raphtory.core.algorithm

import com.raphtory.core.client.QuerySender
import com.raphtory.core.components.querymanager.Query

/**
  * @DoNotDocument
  */
class GenericGraphPerspective(query: Query, private val querySender: QuerySender)
        extends DefaultGraphOperations[GraphPerspective](query, querySender)
        with GraphPerspective {

  override protected def newGraph(query: Query, querySender: QuerySender) =
    new GenericGraphPerspective(query, querySender)
}

package com.raphtory.internals.storage.arrow

import com.raphtory.internals.graph.{GraphPartition, LensInterface}

final case class ArrowGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    var superStep: Int,
    private val storage: GraphPartition
) //extends LensInterface {}

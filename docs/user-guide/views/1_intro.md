# Overview

Many operations are executed on the whole graph, including the full history. This section describes how to use a [GraphView][raphtory.GraphView] to look at a subset of your data without having to re-ingest it.

A `GraphView` is a snapshot of a graph that is used when reading data and running algorithms. Generally, you will use a [Graph][raphtory.Graph] object when building or mutating a graph and for global queries, but use a `GraphView` to extract data from specific regions of interest. You can create `GraphViews` using either time functions or filters to select a narrower subset of your graph. You can then run you queries against the `GraphView` instead of the `Graph` object.

Raphtory can maintain hundreds of thousands of `GraphViews` in parallel and allows chaining view functions together to create as specific a view as is required for your use case. A unified API means that all functions that can be called on a graph, node or edge can also be applied to this subset.

!!! info

    This chapter will continue using the baboon graph described in the [Querying Introduction](../querying/1_intro.md).

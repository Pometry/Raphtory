# Overview

Many operations are executed on the whole graph, including the full history. This section describes how to use [GraphView][raphtory.GraphView]s to look at a subset of your data without having to re-ingest it.

You can create `GraphView`s using either time functions or using filters to select a narrower set of your data.

Raphtory can maintain hundreds of thousands of `GraphViews` in parallel and allows chaining view functions together to create as specific a view as is required for your use case. A unified API means that all functions that can be called on a graph, node or edge can also be applied to this subset.

!!! info

    This chapter will continue using the baboon graph described in the [Querying Introduction](../querying/1_intro.md).

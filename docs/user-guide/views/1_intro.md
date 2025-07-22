# Introduction and dataset

Many operations are executed on the whole graph, including the full history. In this section we will look at applying `Graph Views` which provide a way to look at a subset of this data without having to re-ingest it. 

Raphtory can maintain hundreds of thousands of `Graph Views` in parallel and allows chaining view functions together to create as specific a filter as is required for your use case. A unified API means that all functions that can be called on a graph, node or edge can also be applied to this subset.

!!! info 
    This chapter will continue using the Monkey graph described in the [Querying Introduction](../querying/1_intro.md).

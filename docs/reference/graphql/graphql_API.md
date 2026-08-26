---
hide:
  - navigation
---

<!-- START graphql-markdown -->

# Schema Types


## Query (QueryRoot)
<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.hello">hello</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Hello world demo

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.graph">graph</strong></td>
<td valign="top"><a href="#graph">Graph</a></td>
<td>

Returns a graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace (e.g. `"master"` or `"team/project/graph"`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphType</td>
<td valign="top"><a href="#graphtype">GraphType</a></td>
<td>

Optional override for graph semantics — `EVENT` treats every update as a point-in-time event, `PERSISTENT` carries values forward until overwritten or deleted. Defaults to the stored graph's native type.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.graphmetadata">graphMetadata</strong></td>
<td valign="top"><a href="#metagraph">MetaGraph</a></td>
<td>

Returns lightweight metadata for a graph (node/edge counts, timestamps) without loading it.
Requires at least INTROSPECT permission.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.updategraph">updateGraph</strong></td>
<td valign="top"><a href="#mutablegraph">MutableGraph</a>!</td>
<td>

Update graph query, has side effects to update graph state

Returns:: GqlMutableGraph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.namespaces">namespaces</strong></td>
<td valign="top"><a href="#collectionofnamespace">CollectionOfNamespace</a>!</td>
<td>

Returns all namespaces using recursive search

Returns::  List of namespaces on root

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.namespace">namespace</strong></td>
<td valign="top"><a href="#namespace">Namespace</a>!</td>
<td>

Returns a specific namespace at a given path

Returns:: Namespace or error if no namespace found

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.root">root</strong></td>
<td valign="top"><a href="#namespace">Namespace</a>!</td>
<td>

Returns root namespace

Returns::  Root namespace

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.plugins">plugins</strong></td>
<td valign="top"><a href="#queryplugin">QueryPlugin</a>!</td>
<td>

Returns a plugin.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.receivegraph">receiveGraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Encodes graph and returns as string.

Returns:: Base64 url safe encoded string

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.version">version</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Version string of the running `raphtory-graphql` server build.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.vectorisegraph">vectoriseGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update graph query, has side effects to update graph state

Returns:: GqlMutableGraph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">model</td>
<td valign="top"><a href="#embeddingmodel">EmbeddingModel</a></td>
<td>

Optional embedding model; defaults to OpenAI's standard model.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top"><a href="#template">Template</a></td>
<td>

Optional node-document template (which fields go into each node's text representation); defaults to the built-in template.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">edges</td>
<td valign="top"><a href="#template">Template</a></td>
<td>

Optional edge-document template; defaults to the built-in template.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.vectorisedgraph">vectorisedGraph</strong></td>
<td valign="top"><a href="#vectorisedgraph">VectorisedGraph</a></td>
<td>

Create vectorised graph in the format used for queries

Returns:: GqlVectorisedGraph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
</tbody>
</table>

## Mutation (MutRoot)
<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.plugins">plugins</strong></td>
<td valign="top"><a href="#mutationplugin">MutationPlugin</a>!</td>
<td>

Returns a collection of mutation plugins.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.deletegraph">deleteGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Delete graph from a path on the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.loadnodes">loadNodes</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Load nodes

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dataPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Path to the parquet directory.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The column name for the timestamps.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">id</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The column name for the node IDs.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td>

A value to use as the node type for all nodes. Cannot be used in combination with node_type_col.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypeCol</td>
<td valign="top"><a href="#string">String</a></td>
<td>

The node type column name in a dataframe. Cannot be used in combination with node_type.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of node property column names.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">metadata</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of node metadata column names.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">schema</td>
<td valign="top"><a href="#string">String</a></td>
<td>

A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">eventId</td>
<td valign="top"><a href="#string">String</a></td>
<td>

The column name for the secondary index.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

A value to use as the layer for all nodes. Cannot be used in combination with layer_col.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layerCol</td>
<td valign="top"><a href="#string">String</a></td>
<td>

The node layer column name in a dataframe. Cannot be used in combination with layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.loadedges">loadEdges</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Load edges

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dataPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Path to the parquet directory.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The column name for the update timestamps.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The column name for the source node IDs.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The column name for the destination node IDs.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of edge property column names. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">metadata</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of edge metadata column names. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">schema</td>
<td valign="top"><a href="#string">String</a></td>
<td>

A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">eventId</td>
<td valign="top"><a href="#string">String</a></td>
<td>

The column name for the secondary index.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

A value to use as the layer for all edges. Cannot be used in combination with layer_col. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layerCol</td>
<td valign="top"><a href="#string">String</a></td>
<td>

The edge layer column name in a dataframe. Cannot be used in combination with layer. Defaults to None.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.newgraph">newGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Creates a new graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphType</td>
<td valign="top"><a href="#graphtype">GraphType</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.movegraph">moveGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Move graph from a path on the server to a new_path on the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Current graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, allow replacing an existing graph at `newPath`; defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.copygraph">copyGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Copy graph from a path on the server to a new_path on the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Source graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, allow replacing an existing graph at `newPath`; defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.uploadgraph">uploadGraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Upload a graph file from a path on the client using GQL multipart uploading.

Returns::
name of the new graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graph</td>
<td valign="top"><a href="#upload">Upload</a>!</td>
<td>

Multipart upload of the serialised graph file.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

If true, replace any graph already at `path`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.sendgraph">sendGraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Send graph bincode as base64 encoded string.

Returns::
path of the new graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graph</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Base64-encoded bincode of the serialised graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

If true, replace any graph already at `path`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.createnamespace">createNamespace</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Create an empty namespace at `path`.

Creates any missing parent namespaces along the way. Requires WRITE
permission on the parent namespace. Rejects paths that already host a
graph or an existing namespace, and paths that fail validation.

Returns:: the path of the created namespace

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.deletenamespace">deleteNamespace</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Delete a namespace and all of its descendants (graphs and sub-namespaces).

Requires WRITE permission on the parent namespace, on the namespace
itself, and on every descendant graph and sub-namespace. Cached graphs
at any deleted path are invalidated. Rejects empty and non-existent
paths.

Returns:: true on success

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Path to delete relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.createsubgraph">createSubgraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns a subgraph given a set of nodes from an existing graph in the server.

Returns::
name of the new graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">parentPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Source graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids to include in the subgraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

If true, replace any graph already at `newPath`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.flush">flush</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Flush any pending writes for the graph at `graphPath` to disk.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Graph path relative to the root namespace.

</td>
</tr>
</tbody>
</table>

## Objects

### Algorithms

The algorithms that can be run on a graph view.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.pagerank">pagerank</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the PageRank centrality of every node in the graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of iterations to run. Defaults to 20.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">tol</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Convergence tolerance. Defaults to 0.000001.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dampingFactor</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Probability that the spread continues. Defaults to 0.85.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">weight</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Edge property to use as weight. If unset, all edges have weight 1.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.degreecentrality">degreeCentrality</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the degree centrality of every node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.betweennesscentrality">betweennessCentrality</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the betweenness centrality of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">k</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of nodes to sample. Defaults to all nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">normalized</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Whether to normalize the values. Defaults to true.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.hits">hits</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the HITS hub and authority scores of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of iterations to run. Defaults to 20.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.singlesourceshortestpath">singleSourceShortestPath</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the shortest (unweighted) path from `source` to every reachable node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">source</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">cutoff</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Optional maximum path length; stops the search once reached.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.incomponents">inComponents</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the in component (all nodes that can reach it following out-edges) of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.outcomponents">outComponents</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the out component (all reachable nodes following out-edges) of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.incomponent">inComponent</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the in component of a single node (nodes that can reach it, with their distance).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">node</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.outcomponent">outComponent</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the out component of a single node (nodes it can reach, with their distance).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">node</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.localtrianglecount">localTriangleCount</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the local triangle count of a single node (0 if it has degree < 2), or null if
the node does not exist in the view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">node</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.localclusteringcoefficient">localClusteringCoefficient</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Returns the local clustering coefficient of a single node (0 if it has degree < 2), or
null if the node does not exist in the view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">node</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.weaklyconnectedcomponents">weaklyConnectedComponents</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the weakly connected component id of every node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.stronglyconnectedcomponents">stronglyConnectedComponents</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the strongly connected component id of every node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.louvain">louvain</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the community of every node (Louvain).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">resolution</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Resolution parameter for modularity. Defaults to 1.0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">weightProp</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Edge property to use as weight. If unset, all edges have weight 1.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">tol</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Convergence tolerance. Defaults to 1e-8.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">rngSeed</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Seed for the node-shuffling rng. If unset, seeded from the OS.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.labelpropagation">labelPropagation</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the community of every node (label propagation).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of iterations to run. Defaults to 20.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.dijkstra">dijkstra</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the weighted shortest path from `source` to each of `targets` (Dijkstra).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">source</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">targets</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Target node ids.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">weight</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Edge property to use as weight. If unset, all edges have weight 1.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">direction</td>
<td valign="top"><a href="#direction">Direction</a></td>
<td>

Edge direction to follow. Defaults to BOTH.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.alllocalreciprocity">allLocalReciprocity</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the local reciprocity of every node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.balance">balance</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the net sum of edge weights (balance) of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Edge property to use as weight. Defaults to `weight`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">direction</td>
<td valign="top"><a href="#direction">Direction</a></td>
<td>

Edge direction to consider. Defaults to BOTH.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.localclusteringcoefficientbatch">localClusteringCoefficientBatch</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the local clustering coefficient of each of the given nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids to compute the coefficient for.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.globalclusteringcoefficient">globalClusteringCoefficient</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Returns the global clustering coefficient of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.directedgraphdensity">directedGraphDensity</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Returns the directed graph density (fraction of possible directed edges present).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.globalreciprocity">globalReciprocity</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Returns the global reciprocity of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.averagedegree">averageDegree</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Returns the average (undirected) degree of the graph's nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.maxdegree">maxDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the maximum (undirected) degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.mindegree">minDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the minimum (undirected) degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.maxoutdegree">maxOutDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the maximum out-degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.maxindegree">maxInDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the maximum in-degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.minoutdegree">minOutDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the minimum out-degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.minindegree">minInDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the minimum in-degree of any node in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.tripletcount">tripletCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of connected triplets (paths of length 2) in the graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.trianglecount">triangleCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of triangles in the graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.fastrp">fastRp</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the FastRP embedding of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">embeddingDim</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Dimension of the embedding.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">normalizationStrength</td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Normalization strength applied to neighbour contributions.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterWeights</td>
<td valign="top">[<a href="#float">Float</a>!]!</td>
<td>

Weight of each iteration's contribution to the embedding.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">seed</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Seed for the rng. If unset, seeded from the OS.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.temporallyreachablenodes">temporallyReachableNodes</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the nodes temporally reachable from `seedNodes` starting at `startTime`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">maxHops</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of hops to traverse.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">startTime</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Time at which the traversal starts.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">seedNodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids to start from.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">stopNodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]</td>
<td>

Node ids that halt the traversal when reached.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.fruchtermanreingold">fruchtermanReingold</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the 2D layout position of every node (Fruchterman-Reingold).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of iterations to run. Defaults to 100.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">scale</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Scale of the layout. Defaults to 1.0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeStartSize</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Initial node size. Defaults to 1.0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">cooloffFactor</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Cooloff factor. Defaults to 0.95.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dt</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Time step. Defaults to 0.1.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.cohesivefruchtermanreingold">cohesiveFruchtermanReingold</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the 2D layout position of every node (cohesive Fruchterman-Reingold).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of iterations to run. Defaults to 100.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">scale</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Scale of the layout. Defaults to 1.0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeStartSize</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Initial node size. Defaults to 1.0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">cooloffFactor</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Cooloff factor. Defaults to 0.95.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dt</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Time step. Defaults to 0.1.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.localtemporalthreenodemotifs">localTemporalThreeNodeMotifs</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns the local temporal three-node motif counts of every node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">delta</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum time difference between the first and last edge of a motif.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.globaltemporalthreenodemotif">globalTemporalThreeNodeMotif</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Returns the graph-wide temporal three-node motif counts: 40 counts in a
fixed order (8 two-node, 24 star, then 8 triangle motifs).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">delta</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum time difference between the first and last edge of a motif.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.globaltemporalthreenodemotifmulti">globalTemporalThreeNodeMotifMulti</strong></td>
<td valign="top">[<a href="#motifcounts">MotifCounts</a>!]!</td>
<td>

Returns the graph-wide temporal three-node motif counts for each of
`deltas`, one row of 40 counts per delta, in the order given.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">deltas</td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Maximum time differences to compute the motif counts for.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of threads to use. Defaults to all available.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.temporalrichclubcoefficient">temporalRichClubCoefficient</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Returns the temporal rich club coefficient: the maximal density among the
nodes of degree at least `k` that persists over `windowSize` consecutive
snapshots. The snapshots are the rolling windows described by
`rollingWindow` / `rollingStep`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">k</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Minimum degree a node must have to be in the rich club.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">windowSize</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of consecutive snapshots the edges must persist over.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">rollingWindow</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each snapshot.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">rollingStep</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one snapshot and the next. Defaults to `rollingWindow`, i.e. non-overlapping snapshots.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.temporalseir">temporalSeir</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Simulates an SEIR epidemic, returning the infection, activation and
recovery times of every node that was infected.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">seeds</td>
<td valign="top"><a href="#seeds">Seeds</a>!</td>
<td>

How the initially infected nodes are chosen.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">infectionProb</td>
<td valign="top"><a href="#float">Float</a>!</td>
<td>

Probability that an encounter between an active and a susceptible node infects it.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">initialInfection</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the initial infection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">recoveryRate</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Rate at which infected nodes recover. If unset, nodes never recover.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">incubationRate</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Rate at which infected nodes become infectious. If unset, they are infectious immediately.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">rngSeed</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Seed for the random number generator. If unset, seeded from the OS.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="algorithms.maxweightmatching">maxWeightMatching</strong></td>
<td valign="top"><a href="#matching">Matching</a>!</td>
<td>

Returns a maximum weight matching of the graph, treated as undirected.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">weightProp</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Edge property to use as weight. If unset, all edges have weight 1.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">maxCardinality</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Only consider maximum-cardinality matchings. Defaults to false.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">verifyOptimum</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Verify that the matching found is optimum. Defaults to false.

</td>
</tr>
</tbody>
</table>

### CollectionOfMetaGraph

Collection of items

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="collectionofmetagraph.list">list</strong></td>
<td valign="top">[<a href="#metagraph">MetaGraph</a>!]!</td>
<td>

Returns a list of collection objects.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofmetagraph.page">page</strong></td>
<td valign="top">[<a href="#metagraph">MetaGraph</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofmetagraph.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns a count of collection objects.

</td>
</tr>
</tbody>
</table>

### CollectionOfNamespace

Collection of items

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespace.list">list</strong></td>
<td valign="top">[<a href="#namespace">Namespace</a>!]!</td>
<td>

Returns a list of collection objects.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespace.page">page</strong></td>
<td valign="top">[<a href="#namespace">Namespace</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespace.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns a count of collection objects.

</td>
</tr>
</tbody>
</table>

### CollectionOfNamespacedItem

Collection of items

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespaceditem.list">list</strong></td>
<td valign="top">[<a href="#namespaceditem">NamespacedItem</a>!]!</td>
<td>

Returns a list of collection objects.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespaceditem.page">page</strong></td>
<td valign="top">[<a href="#namespaceditem">NamespacedItem</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="collectionofnamespaceditem.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns a count of collection objects.

</td>
</tr>
</tbody>
</table>

### Document

Document in a vector graph

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="document.entity">entity</strong></td>
<td valign="top"><a href="#documententity">DocumentEntity</a>!</td>
<td>

Entity associated with document.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.content">content</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Content of the document.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.embedding">embedding</strong></td>
<td valign="top">[<a href="#float">Float</a>!]!</td>
<td>

Similarity score with a specified query

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.score">score</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### Edge

Raphtory graph edge.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edge.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Return a view of Edge containing only the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layers">layers</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of Edge containing all layers in the list of names.

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of Edge containing all layers except the excluded list of names.

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layer">layer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of Edge containing the specified layer.

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of Edge containing all layers except the excluded layer specified.

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.rolling">rolling</strong></td>
<td valign="top"><a href="#edgewindowset">EdgeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given window duration and optional step using a rolling window.

A rolling window is a window that moves forward by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.expanding">expanding</strong></td>
<td valign="top"><a href="#edgewindowset">EdgeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given step size using an expanding window.

An expanding window is a window that grows by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.window">window</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events between the specified start (inclusive) and end (exclusive).

For persistent graphs, any edge which exists at any point during the window will be included. You may want to restrict this to only edges that are present at the end of the window using the is_valid function.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.at">at</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latest">latest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

View of this edge pinned to the graph's latest time — equivalent to
`at(graph.latestTime)`. The edge's properties and metadata show their
most recent values, and (for persistent graphs) validity is evaluated
at that instant.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events that are valid at time.

This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events that are valid at the latest time.

This is equivalent to a no-op for Graph and latest() for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.before">before</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events before a specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.after">after</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events after a specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Shrinks both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Set the start of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Set the end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.applyviews">applyViews</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Takes a specified selection of views and applies them in given order.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgeviewcollection">EdgeViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the earliest time of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.firstupdate">firstUpdate</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

The timestamp of the first event in this edge's history (first update, first
deletion, or anything in between). Differs from `earliestTime` in that
`earliestTime` reports when the edge is first *valid*; `firstUpdate` reports
when its history actually begins.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latesttime">latestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.lastupdate">lastUpdate</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

The timestamp of the last event in this edge's history (last update, last
deletion, or anything in between). Differs from `latestTime` in that
`latestTime` reports when the edge is last *valid*; `lastUpdate` reports
when its history actually ends.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.time">time</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the time of an exploded edge. Errors on an unexploded edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the start time for rolling and expanding windows for this edge. Returns none if no window is applied.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the end time of the window. Returns none if no window is applied.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.src">src</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the source node of the edge.

Returns:
Node:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.dst">dst</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the destination node of the edge.

Returns:
Node:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.nbr">nbr</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the node at the other end of the edge (same as dst() for out-edges and src() for in-edges).

Returns:
Node:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.id">id</strong></td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Returns the `[src, dst]` id pair of the edge. Each id is a `String`
for string-indexed graphs or a non-negative `Int` for integer-indexed
graphs.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.properties">properties</strong></td>
<td valign="top"><a href="#properties">Properties</a>!</td>
<td>

Returns a view of the properties of the edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.metadata">metadata</strong></td>
<td valign="top"><a href="#metadata">Metadata</a>!</td>
<td>

Returns the metadata of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layernames">layerNames</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns the names of the layers that have this edge as a member.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layername">layerName</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the layer name of an exploded edge, errors on an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.explode">explode</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns an edge object for each update within the original edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.explodelayers">explodeLayers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns an edge object for each layer within the original edge.

Each new edge object contains only updates from the respective layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.history">history</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a History object with time entries for when an edge is added or change to an edge is made.

Returns:
History:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.deletions">deletions</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a history object with time entries for an edge's deletion times.

Returns:
History:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isvalid">isValid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Checks if the edge is currently valid and exists at the current time.

Returns: boolean

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Checks if the edge is currently active and has at least one update within the current period.

Returns: boolean

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isdeleted">isDeleted</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Checks if the edge is deleted at the current time.

Returns: boolean

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isselfloop">isSelfLoop</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if the edge source and destination nodes are the same.

Returns: boolean

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.filter">filter</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Apply an edge filter in place, returning an edge view whose properties /
metadata / history are restricted to the matching subset.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.event">event</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Pin this edge to a single event — the exploded instance recorded at
the given event time, optionally restricted to a layer. Matches the
members of `explode`: the returned edge answers `time` and `layerName`
and its properties are those of that one update. An Int or DateTime
String matches the first event at that timestamp; pass the object form
`{timestamp, eventId}` to select an exact event when several share a
timestamp. Errors if the edge has no matching event under the current
view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Event time to pin to — Int, DateTime String, or `{timestamp, eventId}`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name the event must belong to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.eventlayer">eventLayer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Pin this edge to a single layer-exploded instance by layer name — the
analogue of `event(...)` for `explodeLayers()`. The returned edge has a
resolved `layerName`; `time` is unavailable on it, matching the local
`explode_layers()` semantics (a layer instance spans all of its events).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to pin to.

</td>
</tr>
</tbody>
</table>

### EdgeSchema

Describes edges between a specific pair of node types — the property and
metadata keys seen on such edges, along with their observed value types.
One `EdgeSchema` per `(srcType, dstType)` pair per layer.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgeschema.srctype">srcType</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the type of source for these edges

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeschema.dsttype">dstType</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the type of destination for these edges

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeschema.properties">properties</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td>

Returns the list of property schemas for edges connecting these types of nodes

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeschema.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td>

Returns the list of metadata schemas for edges connecting these types of nodes

</td>
</tr>
</tbody>
</table>

### EdgeWindowSet

A lazy sequence of per-window views of a single edge, produced by
`edge.rolling` / `edge.expanding`. Each entry is the edge as it exists in
that window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowset.page">page</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowset.list">list</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### Edges

A lazy collection of edges from a graph view. Supports the usual view
transforms (window, layer, filter, ...), plus edge-specific ones like
`explode` and `explodeLayers`, pagination, and sorting.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edges.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a collection containing only edges in the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.layers">layers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a collection containing only edges belonging to the listed layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a collection containing edges belonging to all layers except the excluded list of layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.layer">layer</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a collection containing edges belonging to the specified layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a collection containing edges belonging to all layers except the excluded layer specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.rolling">rolling</strong></td>
<td valign="top"><a href="#edgeswindowset">EdgesWindowSet</a>!</td>
<td>

Creates a WindowSet with the given window duration and optional step using a rolling window. A rolling window is a window that moves forward by step size at each iteration.

Returns a collection of collections. This means that item in the window set is a collection of edges.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.expanding">expanding</strong></td>
<td valign="top"><a href="#edgeswindowset">EdgesWindowSet</a>!</td>
<td>

Creates a WindowSet with the given step size using an expanding window. An expanding window is a window that grows by step size at each iteration.

Returns a collection of collections. This means that item in the window set is a collection of edges.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.window">window</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events between the specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.at">at</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.latest">latest</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

View showing only the latest state of each edge (equivalent to `at(latestTime)`).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events that are valid at time. This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events that are valid at the latest time. This is equivalent to a no-op for Graph and latest() for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.before">before</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events before a specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.after">after</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events after a specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Shrinks both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Set the start of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Set the end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.applyviews">applyViews</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Takes a specified selection of views and applies them in order given.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgesviewcollection">EdgesViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.explode">explode</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Expand each edge into one edge per update: if `A->B` has three updates, it
becomes three `A->B` entries each at a distinct timestamp. Use this to
iterate per-event rather than per-edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.explodelayers">explodeLayers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns an edge object for each layer within the original edge.

Each new edge object contains only updates from the respective layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.sorted">sorted</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Sort the edges. Multiple criteria are applied lexicographically (ties
on the first key break to the second, etc.).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#edgesortby">EdgeSortBy</a>!]!</td>
<td>

Ordered list of sort keys. Each entry chooses exactly one of `src` / `dst` / `neighbour` / `time` / `property`, with an optional `reverse: true` to flip order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the start time of the window or none if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the end time of the window or none if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.src">src</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the source node of each edge, as a flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.dst">dst</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the destination node of each edge, as a flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.nbr">nbr</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the node at the other end of each edge (destination for
out-edges, source for in-edges), as a flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges.

Returns:
int:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.page">page</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.list">list</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Returns a list of all objects in the current selection of the collection. You should filter the collection first then call list.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.filter">filter</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Narrow the collection to edges matching `expr`. The filter sticks to the
returned view — every subsequent traversal through these edges (their
properties, their endpoints' neighbours, etc.) continues to see the
filtered scope.

Useful when you want one scoping rule to apply across the whole query.
E.g. restricting everything to a specific week:

```text
edges { filter(expr: {window: {start: 1234, end: 5678}}) {
list { src { neighbours { list { name } } } }   # neighbours still windowed
} }
```

Contrast with `select`, which applies here and is not carried through.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.select">select</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Narrow the collection to edges matching `expr`, but only at this step —
subsequent traversals out of these edges see the unfiltered graph again.

Useful when you want different scopes at different hops. E.g. Monday's
edges, then the neighbours of their endpoints on Tuesday, then *those*
neighbours on Wednesday:

```text
edges { select(expr: {window: {...monday...}}) {
list { src { select(expr: {window: {...tuesday...}}) {
neighbours { select(expr: {window: {...wednesday...}}) {
neighbours { list { name } }
} }
} } }
} }
```

Contrast with `filter`, which persists the scope through subsequent ops.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
</tbody>
</table>

### EdgesWindowSet

A lazy sequence of per-window edge collections, produced by
`edges.rolling` / `edges.expanding`. Each entry is an `Edges` collection
as it exists in that window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgeswindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeswindowset.page">page</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeswindowset.list">list</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### EventTime

Raphtory’s EventTime.
Represents a unique timepoint in the graph’s history as (timestamp, event_id).

- timestamp: Number of milliseconds since the Unix epoch.
- event_id: ID used for ordering between equal timestamps.

Instances of EventTime may or may not contain time information.
This is relevant for functions that may not return data (such as earliest_time and latest_time) because the data is unavailable.
When empty, time operations (such as timestamp, datetime, and event_id) will return None.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="eventtime.timestamp">timestamp</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Get the timestamp in milliseconds since the Unix epoch.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="eventtime.eventid">eventId</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Get the event id for the EventTime. Used for ordering within the same timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="eventtime.datetime">datetime</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Access a datetime representation of the EventTime as a String.
Useful for converting millisecond timestamps into easily readable datetime strings.
Optionally, a format string can be passed to format the output.
Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
Refer to chrono::format::strftime for formatting specifiers and escape sequences.
Raises an error if a time conversion fails.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">formatString</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional format string for the rendered datetime. Uses `%`-style specifiers — for example `%Y-%m-%d` for `2024-01-15`, `%Y-%m-%d %H:%M:%S` for `2024-01-15 10:30:00`, or `%H:%M` for `10:30`. Defaults to RFC 3339 (e.g. `2024-01-15T10:30:45.123+00:00`) when omitted.

</td>
</tr>
</tbody>
</table>

### Graph

A view of a Raphtory graph. Every field here returns either data from the
view or a derived view (`window`, `layer`, `at`, `filter`, ...) that you can
keep chaining. Views are cheap — they don't copy the underlying data.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graph.uniquelayers">uniqueLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns the names of all layers in the graphview.
Distinct layer names observed in the current view — any layer that has at
least one edge event visible here. Excludes layers that exist elsewhere in
the graph but whose edges have been filtered out.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View restricted to the default layer — where nodes and edges end up
when `addNode` / `addEdge` is called without a `layer` argument.
Useful for separating "unlayered" base-graph events from named-layer
ones.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.layers">layers</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View restricted to the named layers. Updates on any other layer are hidden;
if that leaves a node or edge with no updates left, it disappears from the
view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View with the named layers hidden. Updates on those layers are removed; if
that leaves a node or edge with no updates left, it disappears from the
view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.layer">layer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View restricted to a single layer. Convenience form of
`layers(names: [name])` — updates on any other layer are hidden, and
entities with nothing left disappear.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View with one layer hidden. Convenience form of
`excludeLayers(names: [name])` — updates on that layer are removed, and
entities with nothing left disappear.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.subgraph">subgraph</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View restricted to a chosen set of nodes and the edges between them. Edges
connecting a selected node to a non-selected node are hidden.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids to keep.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.valid">valid</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View containing only valid edges — for persistent graphs this drops edges
whose most recent event is a deletion at the latest time of the current
view (a later re-addition would keep them). On event graphs this is a
no-op.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.subgraphnodetypes">subgraphNodeTypes</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View restricted to nodes with the given node types.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Node types to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludenodes">excludeNodes</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

View with a set of nodes removed (along with any edges touching them).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.rolling">rolling</strong></td>
<td valign="top"><a href="#graphwindowset">GraphWindowSet</a>!</td>
<td>

Creates a rolling window with the specified window size and an optional step.

A rolling window is a window that moves forward by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.expanding">expanding</strong></td>
<td valign="top"><a href="#graphwindowset">GraphWindowSet</a>!</td>
<td>

Creates an expanding window with the specified step size.

An expanding window is a window that grows by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.window">window</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Return a graph containing only the activity between start and end, by default raphtory stores times in milliseconds from the unix epoch.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.at">at</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Creates a view including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.latest">latest</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Creates a view including all events at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Create a view including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Create a view including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.before">before</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Create a view including all events before a specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.after">after</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Create a view including all events after a specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Shrink both the start and end of the window. The new bounds are taken as the
intersection with the current window; this never widens the view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if before the current start.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if after the current end.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Set the start of the window to the larger of the specified value or current start.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); has no effect if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Set the end of the window to the smaller of the specified value or current end.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); has no effect if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.created">created</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Filesystem creation timestamp (epoch millis) of the graph's on-disk folder
— i.e. when this graph was first saved to the server, not when its earliest
event occurred. Use `earliestTime` for the latter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.lastopened">lastOpened</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the graph's last opened timestamp according to system time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.lastupdated">lastUpdated</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the graph's last updated timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the time entry of the earliest activity in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.latesttime">latestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the time entry of the latest activity in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the start time of the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the end time of the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in the graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.earliestedgetime">earliestEdgeTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

The earliest time at which any edge in this graph is valid.

* `includeNegative` — if false, edge events with a timestamp `< 0` are
skipped when computing the minimum. Defaults to true.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If false, edge events with a timestamp `< 0` are skipped when computing the minimum. Defaults to true.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.latestedgetime">latestEdgeTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

The latest time at which any edge in this graph is valid.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If false, edge events with a timestamp `< 0` are skipped when computing the maximum. Defaults to true.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.countedges">countEdges</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges in the graph.

Returns:
int:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.counttemporaledges">countTemporalEdges</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of temporal edges in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.countnodes">countNodes</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of nodes in the graph.

Optionally takes a list of node ids to return a subset.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.hasnode">hasNode</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if a node with the given id exists in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id to look up.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.hasedge">hasEdge</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if an edge exists between `src` and `dst` in this view, optionally
restricted to a single layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional; if provided, only checks whether the edge exists on this layer. If null or omitted, any layer counts.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.node">node</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td>

Look up a single node by id. Returns null if the node doesn't exist in this
view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.nodes">nodes</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

All nodes in this view, optionally narrowed by a filter.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Optional node filter (by name, property, type, etc.). If omitted, every node in the view is returned.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
<td>

Look up a single edge by its endpoint ids. Returns null if no edge exists
between `src` and `dst` in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

All edges in this view, optionally narrowed by a filter.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Optional edge filter (by property, layer, src/dst, etc.). If omitted, every edge in the view is returned.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.properties">properties</strong></td>
<td valign="top"><a href="#properties">Properties</a>!</td>
<td>

Returns the properties of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.metadata">metadata</strong></td>
<td valign="top"><a href="#metadata">Metadata</a>!</td>
<td>

Returns the metadata of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the graph name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns path of graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.namespace">namespace</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns namespace of graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.schema">schema</strong></td>
<td valign="top"><a href="#graphschema">GraphSchema</a>!</td>
<td>

Returns the graph schema.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.algorithms">algorithms</strong></td>
<td valign="top"><a href="#graphalgorithmplugin">GraphAlgorithmPlugin</a>!</td>
<td>

Access registered graph algorithms (PageRank, shortest path, etc.) for this
graph view. The set of available algorithms is defined by the plugin registry
loaded at server startup.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.algorithm">algorithm</strong></td>
<td valign="top"><a href="#algorithms">Algorithms</a>!</td>
<td>

Access the algorithms that can be run on this graph view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.sharedneighbours">sharedNeighbours</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Nodes that are neighbours of every node in `selectedNodes`. Returns the
intersection of each selected node's neighbour set (undirected).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">selectedNodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids whose common neighbours you want. Returns an empty list if `selectedNodes` is empty or any id does not exist.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.findnodes">findNodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

The nodes whose latest property value matches every `(key, value)`
entry in `propertiesDict`. Mirrors the local `Graph.find_nodes`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">propertiesDict</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

`{key, value}` property entries every returned node must match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.findedges">findEdges</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

The edges whose latest property value matches every `(key, value)`
entry in `propertiesDict`. Mirrors the local `Graph.find_edges`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">propertiesDict</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

`{key, value}` property entries every returned edge must match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.getallnodetypes">getAllNodeTypes</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

All node types present in the graph. Mirrors the local
`Graph.get_all_node_types`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.exportto">exportTo</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Copy all nodes and edges of the current graph view into another already-
existing graph stored on the server. The destination graph is preserved
— this only adds; it does not replace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination graph path relative to the root namespace.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.filter">filter</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a filtered view of the graph. Applies a mixed node/edge filter
expression and narrows nodes, edges, and their properties to what matches.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Optional filter expression: node/edge predicates, graph views (window, layer, ...), or and/or/not combinations of them. `and` is an intersection: each leg is evaluated independently and the results intersect — to evaluate a predicate *inside* a view, scope the predicate itself (e.g. a windowed property condition). If omitted, applies the identity filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.applyviews">applyViews</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Apply a list of view operations in the given order and return the
resulting graph view. Lets callers compose multiple view transforms
(window, layer, filter, snapshot, ...) in a single call.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#graphviewcollection">GraphViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant applied to the running result.

</td>
</tr>
</tbody>
</table>

### GraphAlgorithmPlugin

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphalgorithmplugin.pagerank">pagerank</strong></td>
<td valign="top">[<a href="#pagerankoutput">PagerankOutput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">iterCount</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">threads</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">tol</td>
<td valign="top"><a href="#float">Float</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">weight</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphalgorithmplugin.shortest_path">shortest_path</strong></td>
<td valign="top">[<a href="#shortestpathoutput">ShortestPathOutput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">source</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">targets</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">direction</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
</tbody>
</table>

### GraphSchema

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphschema.nodes">nodes</strong></td>
<td valign="top">[<a href="#nodeschema">NodeSchema</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphschema.layers">layers</strong></td>
<td valign="top">[<a href="#layerschema">LayerSchema</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GraphWindowSet

A lazy sequence of graph snapshots produced by `rolling` or `expanding`.
Each entry is a `Graph` at a different window over time. Iterate via
`list` / `page` (or count with `count`). Subsequent view ops apply
per-window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowset.page">page</strong></td>
<td valign="top">[<a href="#graph">Graph</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowset.list">list</strong></td>
<td valign="top">[<a href="#graph">Graph</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### History

History of updates for an object in Raphtory.
Provides access to temporal properties.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="history.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Get the earliest time entry associated with this history or None if the history is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.latesttime">latestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Get the latest time entry associated with this history or None if the history is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.list">list</strong></td>
<td valign="top">[<a href="#eventtime">EventTime</a>!]!</td>
<td>

List all time entries present in this history.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.listrev">listRev</strong></td>
<td valign="top">[<a href="#eventtime">EventTime</a>!]!</td>
<td>

List all time entries present in this history in reverse order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.page">page</strong></td>
<td valign="top">[<a href="#eventtime">EventTime</a>!]!</td>
<td>

Fetch one page of EventTime entries with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.pagerev">pageRev</strong></td>
<td valign="top">[<a href="#eventtime">EventTime</a>!]!</td>
<td>

Fetch one page of EventTime entries with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.isempty">isEmpty</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns True if the history is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.reverse">reverse</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a new history with the iteration order of the entries reversed.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Get the number of entries contained in the history.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.timestamps">timestamps</strong></td>
<td valign="top"><a href="#historytimestamp">HistoryTimestamp</a>!</td>
<td>

Returns a HistoryTimestamp object which accesses timestamps (milliseconds since the Unix epoch)
instead of EventTime entries.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.datetimes">datetimes</strong></td>
<td valign="top"><a href="#historydatetime">HistoryDateTime</a>!</td>
<td>

Returns a HistoryDateTime object which accesses datetimes instead of EventTime entries.
Useful for converting millisecond timestamps into easily readable datetime strings.
Optionally, a format string can be passed to format the output. Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
Refer to chrono::format::strftime for formatting specifiers and escape sequences.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">formatString</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional format string for the rendered datetime. Uses `%`-style specifiers — for example `%Y-%m-%d` for `2024-01-15`, `%Y-%m-%d %H:%M:%S` for `2024-01-15 10:30:00`, or `%H:%M` for `10:30`. Defaults to RFC 3339 (e.g. `2024-01-15T10:30:45.123+00:00`) when omitted.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.eventid">eventId</strong></td>
<td valign="top"><a href="#historyeventid">HistoryEventId</a>!</td>
<td>

Returns a HistoryEventId object which accesses event ids of EventTime entries.
They are used for ordering within the same timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="history.intervals">intervals</strong></td>
<td valign="top"><a href="#intervals">Intervals</a>!</td>
<td>

Inter-event gap analysis for this history. The returned `Intervals`
object exposes each gap (in milliseconds) between consecutive events,
plus summary statistics — `min` / `max` / `mean` / `median` — and
paginated access via `list` / `listRev` / `page` / `pageRev`.

</td>
</tr>
</tbody>
</table>

### HistoryDateTime

History object that provides access to datetimes instead of `EventTime` entries.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="historydatetime.list">list</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

List all datetimes formatted as strings.
If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
will be raised on time conversion error. Defaults to False.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, ignore unconvertible timestamps; if false, raise an error on the first conversion failure. Defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historydatetime.listrev">listRev</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

List all datetimes formatted as strings in reverse chronological order.
If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
will be raised on time conversion error. Defaults to False.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, ignore unconvertible timestamps; if false, raise an error on the first conversion failure. Defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historydatetime.page">page</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Fetch one page of datetimes formatted as string with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
will be raised on time conversion error. Defaults to False.

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, skip timestamps whose conversion fails; if false, raise an error on the first conversion failure. Defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historydatetime.pagerev">pageRev</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Fetch one page of datetimes formatted as string in reverse chronological order with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
will be raised on time conversion error. Defaults to False.

For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

If true, skip timestamps whose conversion fails; if false, raise an error on the first conversion failure. Defaults to false.

</td>
</tr>
</tbody>
</table>

### HistoryEventId

History object that provides access to event ids instead of `EventTime` entries.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="historyeventid.list">list</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List event ids.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historyeventid.listrev">listRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List event ids in reverse order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historyeventid.page">page</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of event ids with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historyeventid.pagerev">pageRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of event ids in reverse chronological order with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
</tbody>
</table>

### HistoryTimestamp

History object that provides access to timestamps (milliseconds since the Unix epoch) instead of `EventTime` entries.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="historytimestamp.list">list</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List all timestamps.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historytimestamp.listrev">listRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List all timestamps in reverse order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historytimestamp.page">page</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of timestamps with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="historytimestamp.pagerev">pageRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of timestamps in reverse order with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
</tbody>
</table>

### Intervals

Provides access to the intervals between temporal entries of an object.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="intervals.list">list</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List time intervals between consecutive timestamps in milliseconds.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.listrev">listRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

List millisecond time intervals between consecutive timestamps in reverse order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.page">page</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of intervals between consecutive timestamps with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.pagerev">pageRev</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Fetch one page of intervals between consecutive timestamps in reverse order with a number of items up to a specified limit,
optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.mean">mean</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Compute the mean interval between consecutive timestamps. Returns None if fewer than 1 timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.median">median</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Compute the median interval between consecutive timestamps. Returns None if fewer than 1 timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.max">max</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Compute the maximum interval between consecutive timestamps. Returns None if fewer than 1 timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="intervals.min">min</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Compute the minimum interval between consecutive timestamps. Returns None if fewer than 1 timestamp.

</td>
</tr>
</tbody>
</table>

### LayerSchema

Describes a single edge layer — its name and the per `(srcType, dstType)`
edge schemas observed within it.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="layerschema.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the name of the layer with this schema

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="layerschema.edges">edges</strong></td>
<td valign="top">[<a href="#edgeschema">EdgeSchema</a>!]!</td>
<td>

Returns the list of edge schemas for this edge layer

</td>
</tr>
</tbody>
</table>

### Matching

A matching of a graph: a set of edges no two of which share a node.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="matching.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges in the matching.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

The edges in the matching.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.src">src</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td>

The node matched to `dst`, null if it is unmatched.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.dst">dst</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td>

The node matched to `src`, null if it is unmatched.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.edgeforsrc">edgeForSrc</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
<td>

The matched edge for `src`, null if it is unmatched.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.edgefordst">edgeForDst</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
<td>

The matched edge for `dst`, null if it is unmatched.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="matching.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Whether the `src` to `dst` edge is part of the matching.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
</tbody>
</table>

### MetaGraph

Lightweight summary of a stored graph — its name, path, counts, and
filesystem timestamps — served without deserializing the full graph.
Useful for listing what's available on the server before committing to a
full load.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.name">name</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Returns the graph name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns path of graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.created">created</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the timestamp for the creation of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.lastopened">lastOpened</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the graph's last opened timestamp according to system time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.lastupdated">lastUpdated</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the graph's last updated timestamp.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.nodecount">nodeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of nodes in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.edgecount">edgeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges in the graph.

Returns:
int:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.metadata">metadata</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td>

Returns the metadata of the graph.

Reads metadata without forcing a full graph load: from the
in-memory cache if the graph is already loaded, otherwise directly
from disk (parquet metadata for parquet-backed graphs, the
`graph_props` segment for disk-backed graphs). This keeps
`MetaGraph.metadata` cheap for namespace listings of many graphs.

</td>
</tr>
</tbody>
</table>

### Metadata

Constant key/value metadata attached to an entity (node, edge, or graph).
Metadata has no timeline — each key maps to exactly one value for the
lifetime of the entity. Separate from `Properties`, which carries
time-varying data.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="metadata.get">get</strong></td>
<td valign="top"><a href="#property">Property</a></td>
<td>

Look up a single metadata value by key. Returns null if no metadata with that
key exists.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The metadata name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if a metadata entry with the given key exists.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The metadata name to look up.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

All metadata keys present on this entity.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td>

All metadata values as `{key, value}` entries.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

Optional whitelist. If provided, only metadata with these keys is returned; if omitted, every metadata entry is returned.

</td>
</tr>
</tbody>
</table>

### MotifCounts

The motif counts for a single delta. Wraps the counts in an object because
the schema builder does not support nested lists of scalars.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="motifcounts.delta">delta</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

The delta these counts were computed for.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="motifcounts.counts">counts</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The 40 motif counts, positionally ordered (see the core docs).

</td>
</tr>
</tbody>
</table>

### MutableEdge

Write-side handle for a single edge — returned from `addEdge` or
`MutableGraph.edge`. Supports adding updates, deletions, and attaching
or updating metadata.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.success">success</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Use to check if adding the edge was successful.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Get the non-mutable edge for querying.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.src">src</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Get the mutable source node of the edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.dst">dst</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Get the mutable destination node of the edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.delete">delete</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Mark this edge as deleted at the given time. Persistent graphs treat this
as a tombstone (the edge becomes invalid from `time` onwards); event
graphs simply log the deletion event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the deletion.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, uses the layer the edge was originally added on (when called after `addEdge`).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to this edge. Errors if any of the keys already exists —
use `updateMetadata` to overwrite. If this is called after `addEdge`,
the layer is inherited and does not need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to set as metadata.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name; defaults to the inherited layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of this edge, overwriting any existing values for the
given keys. If this is called after `addEdge`, the layer is inherited
and does not need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to upsert.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name; defaults to the inherited layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.addupdates">addUpdates</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Append a property update to this edge at a specific time. If called
after `addEdge`, the layer is inherited and does not need to be
specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the update.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Optional `{key, value}` pairs attached to the event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name; defaults to the inherited layer.

</td>
</tr>
</tbody>
</table>

### MutableGraph

Write-enabled handle for a graph. Obtained by calling `updateGraph(path)`
on the root query with a path you have write permission for. Supports
adding nodes and edges (individually or in batches), attaching
properties/metadata, and looking up mutable `node`/`edge` handles. Use the
read-only `graph(path)` resolver for queries.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.graph">graph</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Read-only view of this graph — identical to what you'd get from
`graph(path:)` on the query root. Use this when you want to compose
queries on the graph you've just mutated. `graphType` lets you
re-interpret the graph at query time (see `graph(path:)` for
semantics); defaults to the stored graph's native type.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphType</td>
<td valign="top"><a href="#graphtype">GraphType</a></td>
<td>

Optional override for graph semantics — `EVENT` treats every update as a point-in-time event, `PERSISTENT` carries values forward until overwritten or deleted. Defaults to the stored graph's native type.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.node">node</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a></td>
<td>

Look up an existing node for mutation. Returns null if the node doesn't
exist; use `addNode` or `createNode` to create one.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addnode">addNode</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Add a new node or append an update to an existing one. Upsert semantics:
no error if the node already exists — properties and type are merged.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Optional property updates attached to this event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional node type to assign. If provided, sets the node's type at this event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, the default layer is used.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.createnode">createNode</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Create a new node or fail if it already exists. Strict alternative to
`addNode` — use this when you want to detect collisions.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the create event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Optional property updates attached to this event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional node type to assign. If provided, sets the node's type at this event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, the default layer is used.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addnodes">addNodes</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Batch-add multiple nodes in one call. For each `NodeAddition`, applies every
update it carries (time/properties pairs), then optionally sets its node type
and adds any metadata. On partial failure, returns a `BatchFailures` error
describing which entries failed and why; otherwise returns true.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeaddition">NodeAddition</a>!]!</td>
<td>

List of `NodeAddition` inputs, each specifying a node's name, optional type, layer, per-timestamp updates, and metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.edge">edge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a></td>
<td>

Look up an existing edge for mutation. Returns null if no such edge exists.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addedge">addEdge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a>!</td>
<td>

Add a new edge or append an update to an existing one. Upsert semantics:
safe to call on an edge that already exists — creates missing endpoints if
needed.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Optional property updates attached to this event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, the default layer is used.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addedges">addEdges</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Batch-add multiple edges in one call. For each `EdgeAddition`, applies every
update it carries, then adds any metadata. On partial failure, returns a
`BatchFailures` error describing which entries failed; otherwise returns
true.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">edges</td>
<td valign="top">[<a href="#edgeaddition">EdgeAddition</a>!]!</td>
<td>

List of `EdgeAddition` inputs, each specifying an edge's `src`, `dst`, optional layer, per-timestamp updates, and metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.deleteedge">deleteEdge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a>!</td>
<td>

Mark an edge as deleted at the given time. Persistent graphs treat this
as a tombstone (the edge becomes invalid from `time` onwards); event
graphs simply log the deletion event. Creates the edge first if it did
not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the deletion.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, the default layer is used.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addproperties">addProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add temporal properties to the graph itself (not a node or edge). Each
call records a property update at `t`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the update.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to set.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to the graph itself. Errors if any of the keys already
exists — use `updateMetadata` to overwrite.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to set as metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of the graph itself, overwriting any existing values for
the given keys.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to upsert.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.flush">flush</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Persist any in-memory state for this graph to disk so other
processes attaching a read-only handle observe up-to-date data.

</td>
</tr>
</tbody>
</table>

### MutableNode

Write-side handle for a single node — returned from `addNode`, `createNode`,
or `MutableGraph.node`. Supports adding updates, setting node type, and
attaching or updating metadata.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.success">success</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Use to check if adding the node was successful.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Get the non-mutable Node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to this node. Errors if any of the keys already exists —
use `updateMetadata` to overwrite.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to set as metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.setnodetype">setNodeType</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Set this node's type. Errors if the node already has a non-default
type and you're trying to change it.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newType</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Node-type name to assign.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of this node, overwriting any existing values for the
given keys.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td>

List of `{key, value}` pairs to upsert.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.addupdates">addUpdates</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Append a property update to this node at a specific time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the update.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Optional `{key, value}` pairs attached to the event.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td>

Optional layer name. If omitted, the default layer is used.

</td>
</tr>
</tbody>
</table>

### MutationPlugin

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="mutationplugin.noops">NoOps</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### Namespace

A directory-like container for graphs and nested namespaces. Graphs are
addressed by path (e.g. `"team/project/graph"`), and every segment except
the last is a namespace. Use to browse what's stored on the server without
loading any graph data.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="namespace.graphs">graphs</strong></td>
<td valign="top"><a href="#collectionofmetagraph">CollectionOfMetaGraph</a>!</td>
<td>

Graphs directly inside this namespace (excludes graphs in nested
namespaces). Filtered by the caller's permissions — only graphs the
caller is allowed to see are returned.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Path of this namespace relative to the root namespace. Empty string for
the root namespace itself.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.parent">parent</strong></td>
<td valign="top"><a href="#namespace">Namespace</a></td>
<td>

Parent namespace, or null at the root.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.children">children</strong></td>
<td valign="top"><a href="#collectionofnamespace">CollectionOfNamespace</a>!</td>
<td>

Sub-namespaces directly inside this one (one level down, not recursive).
Filtered by permissions.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.items">items</strong></td>
<td valign="top"><a href="#collectionofnamespaceditem">CollectionOfNamespacedItem</a>!</td>
<td>

Everything in this namespace — sub-namespaces and graphs — as a single
heterogeneous collection. Sub-namespaces are listed before graphs.
Filtered by permissions.

</td>
</tr>
</tbody>
</table>

### NestedEdges

A nested collection of edges anchored to a source collection — the result of
collection-level traversals like `nodes.edges`, `inEdges`, or `outEdges`.
Each source node yields its own list of incident edges, so results are
shaped as a list of per-source edge collections. Supports the usual view
transforms (window, layer, filter, ...).

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns a collection containing only edges in the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.layers">layers</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns a collection containing only edges belonging to the listed layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns a collection containing edges belonging to all layers except the excluded list of layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.layer">layer</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns a collection containing edges belonging to the specified layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns a collection containing edges belonging to all layers except the excluded layer specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.window">window</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events between the specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.at">at</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.latest">latest</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

View showing only the latest state of each edge (equivalent to `at(latestTime)`).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events that are valid at time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.before">before</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events before a specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.after">after</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Creates a view of the NestedEdges including all events after a specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Shrinks both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Set the start of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Set the end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.applyviews">applyViews</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Takes a specified selection of views and applies them in order given.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgesviewcollection">EdgesViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the earliest time that this NestedEdges is valid or None if valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time that this NestedEdges is valid or None if valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.src">src</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the source node of each edge, grouped per source node, as a
nested `PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.dst">dst</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the destination node of each edge, grouped per source node, as a
nested `PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.nbr">nbr</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the node at the other end of each edge (destination for
out-edges, source for in-edges), grouped per source node, as a nested
`PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.explode">explode</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Expand each source's edges into one edge per update — mirrors the local
`NestedEdges.explode`. The per-source nesting is preserved; only the
inner edge lists fan out per event.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.explodelayers">explodeLayers</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Expand each source's edges into one edge per layer — mirrors the local
`NestedEdges.explode_layers`. Each resulting edge carries only the
updates from its respective layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of source edge collections in this collection (one per source node).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.page">page</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td>

Fetch one page of source edge collections up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 source collections, offset by 11 (2 pages of 5 + 1),
will be returned. Each entry is the per-source list of incident edges.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of source edge collections to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra source edge collections to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.list">list</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td>

Materialise every source edge collection — one `Edges` per source node,
each holding that source's incident-edge list. Read
`list { list { src { name } dst { name } } }` to reach the per-source
edges. Rejected by the server when bulk list endpoints are disabled; use
`page` for paginated access instead.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.filter">filter</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Narrow the edge set to edges matching `expr`. The filter sticks to the
returned collection — every subsequent traversal continues to see the
filtered scope.

Contrast with `select`, which applies here and is not carried through.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nestededges.select">select</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Narrow the edge set to edges matching `expr`, but only at this hop —
further traversals out of these edges see the unfiltered graph again.

Contrast with `filter`, which persists the scope through subsequent ops.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
</tbody>
</table>

### Node

Raphtory graph node.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="node.id">id</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Returns the unique id of the node — `String` for string-indexed
graphs, non-negative `Int` for integer-indexed graphs.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the name of the node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Return a view of the node containing only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.layers">layers</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Return a view of node containing all layers specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns a collection containing nodes belonging to all layers except the excluded list of layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.layer">layer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns a collection containing nodes belonging to the specified layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns a collection containing nodes belonging to all layers except the excluded layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.rolling">rolling</strong></td>
<td valign="top"><a href="#nodewindowset">NodeWindowSet</a>!</td>
<td>

Creates a WindowSet with the specified window size and optional step using a rolling window.

A rolling window is a window that moves forward by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.expanding">expanding</strong></td>
<td valign="top"><a href="#nodewindowset">NodeWindowSet</a>!</td>
<td>

Creates a WindowSet with the specified step size using an expanding window.

An expanding window is a window that grows by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.window">window</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events between the specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.at">at</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.latest">latest</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.before">before</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events before specified end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.after">after</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Create a view of the node including all events after the specified start time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Shrink a Window to a specified start and end time, if these are earlier and later than the current start and end respectively.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Set the start of the window to the larger of a specified start time and self.start().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Set the end of the window to the smaller of a specified end and self.end().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.applyviews">applyViews</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#nodeviewcollection">NodeViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the earliest time that the node exists.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.firstupdate">firstUpdate</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the time of the first update made to the node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.latesttime">latestTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time that the node exists.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.lastupdate">lastUpdate</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the time of the last update made to the node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Gets the start time for the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Gets the end time for the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.history">history</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a history object for the node, with time entries for node additions and changes made to node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.edgehistorycount">edgeHistoryCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Get the number of edge events for this node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if the node is active and it's history is not empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.nodetype">nodeType</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Returns the type of node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.properties">properties</strong></td>
<td valign="top"><a href="#properties">Properties</a>!</td>
<td>

Returns the properties of the node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.metadata">metadata</strong></td>
<td valign="top"><a href="#metadata">Metadata</a>!</td>
<td>

Returns the metadata of the node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.degree">degree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of unique counter parties for this node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outdegree">outDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number edges with this node as the source.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.indegree">inDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number edges with this node as the destination.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.incomponent">inComponent</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Nodes that can reach this one via out-edges. `select` is a general filter expression — a node
filter, an edge filter, or a graph (layer/window) filter — scoping which nodes/edges the walk
steps through. The returned nodes are on the full graph so their other-layer neighbours stay
queryable.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outcomponent">outComponent</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Nodes reachable from this one via out-edges. `select` is a general filter expression — a node
filter, an edge filter, or a graph (layer/window) filter — scoping which nodes/edges the walk
steps through. The returned nodes are on the full (unfiltered) graph, so their other-layer
neighbours remain queryable.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns all connected edges.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outedges">outEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns outgoing edges.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inedges">inEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns incoming edges.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.neighbours">neighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns neighbouring nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the number of neighbours that have at least one in-going edge to this node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the number of neighbours that have at least one out-going edge from this node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.filter">filter</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
</tbody>
</table>

### NodeSchema

Describes nodes of a specific type in a graph — its property keys and
observed value types (and, for string-valued properties, the set of
distinct values seen). One `NodeSchema` per node type.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodeschema.typename">typeName</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The node type this schema describes (e.g. `"person"`, `"org"`).
Falls back to the default node type for untyped nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeschema.properties">properties</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td>

Property schemas seen on nodes of this type — one entry per property key
ever set on a node of this type, with its observed `PropertyType` and (for
string-valued properties) the set of distinct values.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeschema.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td>

Metadata schemas seen on nodes of this type — like `properties`, but
covering metadata fields rather than temporal properties.

</td>
</tr>
</tbody>
</table>

### NodeState

A mapping from the nodes of a graph to the values computed for them by an algorithm.

The output is columnar: every column of the underlying node state is exposed as
a `NodeStateColumn` whose `values` are row-aligned with `nodes`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of nodes with a value in this node state.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.nodes">nodes</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

The nodes with a value in this node state, in row order. Aligned with `values`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.columnnames">columnNames</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

The column names of this node state in order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.rows">rows</strong></td>
<td valign="top">[<a href="#nodestaterow">NodeStateRow</a>!]!</td>
<td>

All rows of the node state keyed by node, with one entry per column.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.headlessrows">headlessRows</strong></td>
<td valign="top">[<a href="#nodestateheadlessrow">NodeStateHeadlessRow</a>!]!</td>
<td>

All rows of the node state keyed by node, without the column names: the `values` of each row are
in `columnNames` order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.get">get</strong></td>
<td valign="top">[<a href="#nodestateentry">NodeStateEntry</a>!]</td>
<td>

Returns the values for a node, one entry per column; null if the node has no value in this NodeState.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">node</td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.min">min</strong></td>
<td valign="top"><a href="#nodestateitem">NodeStateItem</a></td>
<td>

Minimum `(node, value)` of a column. Null if the column does not exist, is empty,
or its values are not comparable (e.g. contains nodes).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.max">max</strong></td>
<td valign="top"><a href="#nodestateitem">NodeStateItem</a></td>
<td>

Maximum `(node, value)` of a column. Null if the column does not exist, is empty,
or its values are not comparable (e.g. contains nodes).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.sum">sum</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

Sum of a column's values, skipping empty cells. Null if the column does not exist, is empty,
or is not additive (e.g. contains nodes).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.mean">mean</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

Mean of a column's values as a float, skipping empty cells. Null if the column does not exist,
is empty, or has any non-numeric value.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.median">median</strong></td>
<td valign="top"><a href="#nodestateitem">NodeStateItem</a></td>
<td>

Median `(node, value)` of a column (upper median on even lengths). Null if the column
does not exist, is empty, or is not comparable (e.g. contains nodes).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.topk">topK</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a></td>
<td>

Returns the `k` rows with the largest values in a column. Empty cells rank
lowest, so they are only included if fewer than `k` rows have a value.
Null if the column does not exist, is empty, or is not comparable.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">k</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of rows to return.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.bottomk">bottomK</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a></td>
<td>

Returns the `k` rows with the smallest values in a column. Empty cells rank
highest, so they are only included if fewer than `k` rows have a value.
Null if the column does not exist, is empty, or is not comparable.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">k</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of rows to return.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.sortbyvalues">sortByValues</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a></td>
<td>

Returns a view of this node state with the rows sorted by a column's values,
ascending with empty cells last. `reverse` flips the whole ordering, putting
empty cells first. Null if the column does not exist, is empty, or is not
comparable.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">reverse</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Sort in descending order instead. Defaults to false.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.groupby">groupBy</strong></td>
<td valign="top">[<a href="#nodestategroup">NodeStateGroup</a>!]</td>
<td>

Groups the nodes by their value in a column. Nodes with an empty cell form
their own group. Null if the column does not exist or contains nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">column</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Column name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.page">page</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns one page of this node state as a node state of its own, so that
`nodes` / `rows` / `columns` on it stay row-aligned with each other.
Pages past the end are empty rather than an error.

For example, if page(limit: 5, offset: 1, page_index: 2) is called, a page with 5 items,
offset by 11 items (2 pages of 5 + 1), will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of rows to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra rows to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.sortbyid">sortById</strong></td>
<td valign="top"><a href="#nodestate">NodeState</a>!</td>
<td>

Returns a view of this node state with the rows sorted by node id.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestate.columns">columns</strong></td>
<td valign="top">[<a href="#nodestatecolumn">NodeStateColumn</a>!]!</td>
<td>

The columns of the node state, one per output field of the algorithm.
`values` are row-aligned with `nodes`.

</td>
</tr>
</tbody>
</table>

### NodeStateColumn

One column of a node state: the values of a single output field of the
algorithm. Row-aligned with `NodeState.nodes`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestatecolumn.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Name of the column.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestatecolumn.values">values</strong></td>
<td valign="top">[<a href="#nodestatevalue">NodeStateValue</a>!]!</td>
<td>

The values of this column; `values[i]` belongs to `NodeState.nodes[i]`.

</td>
</tr>
</tbody>
</table>

### NodeStateEntry

One column's value for a single node.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestateentry.columnname">columnName</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Name of the column.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestateentry.value">value</strong></td>
<td valign="top"><a href="#nodestatevalue">NodeStateValue</a>!</td>
<td>

The node's value in this column.

</td>
</tr>
</tbody>
</table>

### NodeStateGroup

The nodes sharing one value of a column, as returned by `groupBy`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestategroup.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

The value shared by the nodes in this group; null if their cell is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestategroup.nodes">nodes</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

The nodes holding that value.

</td>
</tr>
</tbody>
</table>

### NodeStateHeadlessRow

A node's full row in the node state without the column names: `values[i]`
belongs to the column `NodeState.columnNames[i]`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestateheadlessrow.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

The node this row belongs to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestateheadlessrow.values">values</strong></td>
<td valign="top">[<a href="#nodestatevalue">NodeStateValue</a>!]!</td>
<td>

The row's values, in `columnNames` order.

</td>
</tr>
</tbody>
</table>

### NodeStateItem

A `(node, value)` pair, e.g. the result of a column aggregate.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestateitem.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

The node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestateitem.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a>!</td>
<td>

The node's value.

</td>
</tr>
</tbody>
</table>

### NodeStateProp

A plain property value of a node state cell.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestateprop.prop">prop</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

The property value; null if the node has no value in this column.

</td>
</tr>
</tbody>
</table>

### NodeStateRow

A node's full row in the node state: one entry per column.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodestaterow.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

The node this row belongs to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodestaterow.entries">entries</strong></td>
<td valign="top">[<a href="#nodestateentry">NodeStateEntry</a>!]!</td>
<td>

The row's values, one entry per column.

</td>
</tr>
</tbody>
</table>

### NodeWindowSet

A lazy sequence of per-window views of a single node, produced by
`node.rolling` / `node.expanding`. Each entry is the node as it exists in
that window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowset.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowset.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### Nodes

A lazy collection of nodes from a graph view. Supports all the same view
transforms as `Graph` (window, layer, filter, ...) plus pagination and
sorting. Iterated via `list` / `page` / `ids` / `count`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodes.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Return a view of the nodes containing only the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.layers">layers</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Return a view of the nodes containing all layers specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Return a view of the nodes containing all layers except those specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.layer">layer</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Return a view of the nodes containing the specified layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Return a view of the nodes containing all layers except those specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.rolling">rolling</strong></td>
<td valign="top"><a href="#nodeswindowset">NodesWindowSet</a>!</td>
<td>

Creates a WindowSet with the specified window size and optional step using a rolling window.

A rolling window is a window that moves forward by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.expanding">expanding</strong></td>
<td valign="top"><a href="#nodeswindowset">NodesWindowSet</a>!</td>
<td>

Creates a WindowSet with the specified step size using an expanding window.

An expanding window is a window that grows by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.window">window</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the node including all events between the specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.at">at</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events at a specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.latest">latest</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.before">before</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events before specified end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.after">after</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Create a view of the nodes including all events after the specified start time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Shrink both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Set the start of the window to the larger of a specified start time and self.start().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Set the end of the window to the smaller of a specified end and self.end().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Filter nodes by node type.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Node-type names to keep.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.applyviews">applyViews</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Apply a list of views in the given order and return the resulting nodes
collection. Lets callers compose window, layer, filter, and snapshot
operations in a single call.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#nodesviewcollection">NodesViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, etc.) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.sorted">sorted</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Sort the nodes. Multiple criteria are applied lexicographically (ties on the
first key break to the second, etc.).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#nodesortby">NodeSortBy</a>!]!</td>
<td>

Ordered list of sort keys. Each entry chooses exactly one of `id` / `name` / `type` / `time` / `property`, with an optional `reverse: true` to flip order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the start time of the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the end time of the window. Errors if there is no window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.degree">degree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The degree (number of incident edges) of every node, in collection order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.indegree">inDegree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The in-degree (number of incoming edges) of every node, in collection order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.outdegree">outDegree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The out-degree (number of outgoing edges) of every node, in collection order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.edgehistorycount">edgeHistoryCount</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The number of edge updates incident to every node, in collection order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of nodes in the current view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Materialise every node in the view. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Every node's id (name) as a flat list of strings. Rejected by the server when
bulk list endpoints are disabled.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.filter">filter</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Narrow the collection to nodes matching `expr`. The filter sticks to the
returned view — every subsequent traversal through these nodes (their
neighbours, edges, properties) continues to see the filtered scope.

Useful when you want one scoping rule to apply across the whole query.
E.g. restricting everything to a specific week:

```text
nodes { filter(expr: {window: {start: 1234, end: 5678}}) {
list { neighbours { list { name } } }   # neighbours still windowed
} }
```

Contrast with `select`, which applies here and is not carried through.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.select">select</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Narrow the collection to nodes matching `expr`, but only at this step —
subsequent traversals out of these nodes see the unfiltered graph again.

Useful when you want different scopes at different hops. E.g. nodes
active on Monday, then their neighbours active on Tuesday, then *those*
neighbours active on Wednesday:

```text
nodes { select(expr: {window: {...monday...}}) {
list { neighbours { select(expr: {window: {...tuesday...}}) {
list { neighbours { select(expr: {window: {...wednesday...}}) {
list { name }
} } }
} } }
} }
```

Contrast with `filter`, which persists the scope through subsequent ops.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node predicates, graph views, or and/or/not combinations (and = intersection). Expressions that test edges are rejected.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.neighbours">neighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the neighbouring nodes of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the in-neighbours of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the out-neighbours of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.edges">edges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the incident edges (both directions) of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.inedges">inEdges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the incoming edges of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.outedges">outEdges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the outgoing edges of each node in the collection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
</tbody>
</table>

### NodesWindowSet

A lazy sequence of per-window node collections, produced by
`nodes.rolling` / `nodes.expanding`. Each entry is a `Nodes` collection
as it exists in that window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodeswindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeswindowset.page">page</strong></td>
<td valign="top">[<a href="#nodes">Nodes</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeswindowset.list">list</strong></td>
<td valign="top">[<a href="#nodes">Nodes</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### PagerankOutput

PageRank score.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="pagerankoutput.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pagerankoutput.rank">rank</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### PathFromGraph

A nested collection of nodes anchored to a source collection — the result of
collection-level traversals like `nodes.neighbours`, `inNeighbours`, or
`outNeighbours`. Each source node yields its own list of neighbour nodes, so
results are shaped as a list of per-source node lists. Supports all the usual
view transforms (window, layer, filter, ...) and can be chained to walk
further hops.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.layers">layers</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns a view of PathFromGraph containing the specified layers, errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Return a view of PathFromGraph restricted to the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Return a view of PathFromGraph containing all layers except the specified excluded layers, errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.layer">layer</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Return a view of PathFromGraph containing the specified layer, errors if the layer does not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Return a view of PathFromGraph containing all layers except the specified excluded layer, errors if the layer does not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.window">window</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events between a specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.at">at</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events at time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.latest">latest</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.before">before</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events before the specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.after">after</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Create a view of the PathFromGraph including all events after the specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Shrink both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Set the start of the window to the larger of the specified start and self.start().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Set the end of the window to the smaller of the specified end and self.end().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Narrow this path to neighbours whose node type is in the given set.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Node types to keep.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the earliest time that this PathFromGraph is valid or None if the PathFromGraph is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time that this PathFromGraph is valid or None if the PathFromGraph is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.combinedhistory">combinedHistory</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a single history object combining the time entries of all nodes in this view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of source paths in this collection (one per source node).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.page">page</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td>

Fetch one page of source paths up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 source paths, offset by 11 (2 pages of 5 + 1),
will be returned. Each entry is the per-source list of neighbour nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of source paths to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra source paths to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.list">list</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td>

Materialise every source path — one `PathFromNode` per source node,
each holding that source's neighbour list. Read `list { ids }` /
`list { list { name } }` to reach the per-source neighbours. Rejected
by the server when bulk list endpoints are disabled; use `page` for
paginated access instead.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.ids">ids</strong></td>
<td valign="top"><a href="#nestedstringlist">NestedStringList</a>!</td>
<td>

Columnar `ids`: every source node's neighbour ids as `[[String]]`,
computed in ONE `blocking_compute`. Fast-path equivalent of
`list { ids }`, which resolves one `PathFromNode` object — and its own
`blocking_compute` — per source.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.sourceids">sourceIds</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Columnar `sourceIds`: the id of the source node each path hangs off, in
the same order as `ids` / `list` — one entry per source path, so entry
`i` of `sourceIds` and entry `i` of `ids` describe the same pair. Lets a
client reconstruct the `(source, path)` pairing in ONE request instead of
one request per source. Computed in ONE `blocking_compute`, like `ids`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.degree">degree</strong></td>
<td valign="top"><a href="#nestedintlist">NestedIntList</a>!</td>
<td>

Columnar `degree`: each source node's per-neighbour degrees as `[[Int]]`,
computed in ONE `blocking_compute`. Fast-path for `list { degree }`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.indegree">inDegree</strong></td>
<td valign="top"><a href="#nestedintlist">NestedIntList</a>!</td>
<td>

Columnar `inDegree`. Fast-path for `list { inDegree }`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.outdegree">outDegree</strong></td>
<td valign="top"><a href="#nestedintlist">NestedIntList</a>!</td>
<td>

Columnar `outDegree`. Fast-path for `list { outDegree }`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.applyviews">applyViews</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Takes a specified selection of views and applies them in given order.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#pathfromnodeviewcollection">PathFromNodeViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.filter">filter</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Narrow the neighbour set to nodes matching `expr`. The filter sticks to
the returned path — every subsequent traversal (further hops, edges,
properties) continues to see the filtered scope.

Contrast with `select`, which applies here and is not carried through.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.select">select</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Narrow the neighbour set to nodes matching `expr`, but only at this hop
— further traversals out of these nodes see the unfiltered graph again.

Contrast with `filter`, which persists the scope through subsequent ops.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node predicates, graph views, or and/or/not combinations (and = intersection). Expressions that test edges are rejected.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.neighbours">neighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the neighbouring nodes reachable one further hop from each source
path (both directions), as a nested `PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the in-neighbours reachable one further hop from each source
path, as a nested `PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#pathfromgraph">PathFromGraph</a>!</td>
<td>

Returns the out-neighbours reachable one further hop from each source
path, as a nested `PathFromGraph`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.edges">edges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the incident edges (both directions) of each source path, as a
nested `NestedEdges` collection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.inedges">inEdges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the incoming edges of each source path, as a nested `NestedEdges`
collection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromgraph.outedges">outEdges</strong></td>
<td valign="top"><a href="#nestededges">NestedEdges</a>!</td>
<td>

Returns the outgoing edges of each source path, as a nested `NestedEdges`
collection.

</td>
</tr>
</tbody>
</table>

### PathFromNode

A collection of nodes anchored to a source node — the result of traversals
like `node.neighbours`, `inNeighbours`, or `outNeighbours`. Supports all
the usual view transforms (window, layer, filter, ...) and can be chained
to walk further hops.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.layers">layers</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns a view of PathFromNode containing the specified layer, errors if the layer does not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Return a view of PathFromNode restricted to the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.layer">layer</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Return a view of PathFromNode containing the layer specified layer, errors if the layer does not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Layer name to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.rolling">rolling</strong></td>
<td valign="top"><a href="#pathfromnodewindowset">PathFromNodeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given window size and optional step using a rolling window.

A rolling window is a window that moves forward by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
e.g. "1 month and 1 day" will align at the start of the day.
Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
the start of the first window and/or after the end of the last window (i.e. not included in any window).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td>

Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.expanding">expanding</strong></td>
<td valign="top"><a href="#pathfromnodewindowset">PathFromNodeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given step size using an expanding window.

An expanding window is a window that grows by step size at each iteration.

alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
e.g. "1 month and 1 day" will align at the start of the day.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td>

How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td>

Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.window">window</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events between a specified start (inclusive) and end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.at">at</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events at time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant to pin the view to.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events that are valid at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Instant at which entities must be valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.latest">latest</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.before">before</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events before the specified end (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.after">after</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Create a view of the PathFromNode including all events after the specified start (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive lower bound.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Shrink both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Set the start of the window to the larger of the specified start and self.start().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new start (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Set the end of the window to the smaller of the specified end and self.end().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Proposed new end (TimeInput); ignored if it would widen the window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Narrow this path to neighbours whose node type is in the given set.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Node types to keep.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.start">start</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the earliest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.end">end</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.windowsize">windowSize</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.haslayer">hasLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if a layer with the given name is present in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.combinedhistory">combinedHistory</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Returns a single history object combining the time entries of all nodes reachable from the source in this view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.degree">degree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The degree (number of incident edges) of every node in the path, in order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.indegree">inDegree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The in-degree (number of incoming edges) of every node in the path, in order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.outdegree">outDegree</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The out-degree (number of outgoing edges) of every node in the path, in order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.edgehistorycount">edgeHistoryCount</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

The number of edge updates incident to every node in the path, in order.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of neighbour nodes reachable from the source in this view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Materialise every neighbour node in the path. Rejected by the server when
bulk list endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Every neighbour node's id (name) as a flat list of strings. Rejected by the
server when bulk list endpoints are disabled.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.applyviews">applyViews</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Takes a specified selection of views and applies them in given order.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#pathfromnodeviewcollection">PathFromNodeViewCollection</a>!]!</td>
<td>

Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.filter">filter</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Narrow the neighbour set to nodes matching `expr`. The filter sticks to
the returned path — every subsequent traversal (further hops, edges,
properties) continues to see the filtered scope.

Useful when you want one scoping rule to apply across the whole query.
E.g. restricting the whole traversal to a specific week:

```text
node(name: "A") { neighbours { filter(expr: {window: {...week...}}) {
list { neighbours { list { name } } }   # further hops still windowed
} } }
```

Contrast with `select`, which applies here and is not carried through.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.select">select</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Narrow the neighbour set to nodes matching `expr`, but only at this hop
— further traversals out of these nodes see the unfiltered graph again.

Useful when each hop needs a different scope. E.g. neighbours active on
Monday, then *their* neighbours active on Tuesday:

```text
node(name: "A") { neighbours { select(expr: {window: {...monday...}}) {
list { neighbours { select(expr: {window: {...tuesday...}}) {
list { name }
} } }
} } }
```

Contrast with `filter`, which persists the scope through subsequent ops.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a>!</td>
<td>

Filter expression: node predicates, graph views, or and/or/not combinations (and = intersection). Expressions that test edges are rejected.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.neighbours">neighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the neighbouring nodes reachable one further hop from this path
(both directions), as a flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the in-neighbours reachable one further hop from this path, as a
flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns the out-neighbours reachable one further hop from this path, as a
flat `PathFromNode`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns the incident edges (both directions) of the nodes in this path,
as a flat `Edges` collection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.inedges">inEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns the incoming edges of the nodes in this path, as a flat `Edges`
collection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.outedges">outEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns the outgoing edges of the nodes in this path, as a flat `Edges`
collection.

</td>
</tr>
</tbody>
</table>

### PathFromNodeWindowSet

A lazy sequence of per-window neighbour sets, produced by
`neighbours.rolling` / `neighbours.expanding` (or the in/out variants).
Each entry is a `PathFromNode` scoped to that window.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodewindowset.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of windows in this set. Materialising all windows is expensive for
large graphs — prefer `page` over `list` when iterating.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodewindowset.page">page</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td>

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of items to return on this page.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Extra items to skip on top of `pageIndex` paging (default 0).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Zero-based page number; multiplies `limit` to determine where to start (default 0).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodewindowset.list">list</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td>

Materialise every window as a list. Rejected by the server when bulk list
endpoints are disabled; use `page` for paginated access instead.

</td>
</tr>
</tbody>
</table>

### Properties

All temporal properties of an entity (metadata is exposed separately).
Look up individual properties via `get` / `contains`, enumerate via
`keys` / `values`, or drop into `temporal` for time-aware accessors.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="properties.get">get</strong></td>
<td valign="top"><a href="#property">Property</a></td>
<td>

Look up a single property by key. Returns null if no property with that key
exists in the current view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if a property with the given key exists in this view.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property name to look up.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.getdtypeof">getDtypeOf</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

The data-type of the property's latest value by key, as its `PropType`
display string (e.g. `"I64"`, `"Str"`, `"List<F64>"`). Returns null when
the key isn't present. Mirrors the local `Properties.get_dtype_of`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

All property keys present in the current view. Does not include metadata
— metadata is exposed separately via the entity's `metadata` field.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td>

Snapshot of property values, one `{key, value}` entry per property.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

Optional whitelist. If provided, only properties with these keys are returned; if omitted or null, every property in the view is returned.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.temporal">temporal</strong></td>
<td valign="top"><a href="#temporalproperties">TemporalProperties</a>!</td>
<td>

The temporal-only view of these properties — excludes metadata (which has no
history) and lets you drill into per-key timelines and aggregates.

</td>
</tr>
</tbody>
</table>

### Property

A single `(key, value)` property reading at a point in the graph view.
The value is exposed both as a typed scalar (`value`) and as a
human-readable string (`asString`).

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="property.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property key (name).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="property.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property value rendered as a human-readable string (e.g. `"10"`, `"hello"`,
`"2024-01-01T00:00:00Z"`). For programmatic access use `value`, which returns
a typed scalar.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="property.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a>!</td>
<td>

The property value as a typed `PropertyOutput` scalar — numbers come back as
numbers, booleans as booleans, strings as strings, etc.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="property.dtype">dtype</strong></td>
<td valign="top"><a href="#propertytype">PropertyType</a>!</td>
<td>

The property's exact type, for type-directed decoding of `value`
(`value` alone collapses e.g. all integer widths to one JSON number).

</td>
</tr>
</tbody>
</table>

### PropertySchema

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="propertyschema.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyschema.propertytype">propertyType</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyschema.variants">variants</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### PropertyTuple

A `(time, value)` pair — the output type of temporal-property accessors
that need to report *when* a value was observed (e.g. `min`, `max`,
`median`, `orderedDedupe`).

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.time">time</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

The timestamp at which this value was recorded.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The value rendered as a human-readable string. For programmatic access use
`value`, which returns a typed scalar.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a>!</td>
<td>

The value as a typed `PropertyOutput` scalar — numbers come back as numbers,
booleans as booleans, etc.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.dtype">dtype</strong></td>
<td valign="top"><a href="#propertytype">PropertyType</a>!</td>
<td>

The value's exact type, for type-directed decoding of `value`.

</td>
</tr>
</tbody>
</table>

### QueryPlugin

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="queryplugin.noops">NoOps</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### ShortestPathOutput

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="shortestpathoutput.target">target</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="shortestpathoutput.nodes">nodes</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### TemporalProperties

The temporal-only view of an entity's properties. Each entry is a
`TemporalProperty` carrying the full timeline for that key — use this when
you need per-update iteration, time-indexed lookups, or aggregates.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.get">get</strong></td>
<td valign="top"><a href="#temporalproperty">TemporalProperty</a></td>
<td>

Look up a single temporal property by key. Returns null if there's no temporal
property with that key.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property name.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if a temporal property with the given key exists.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property name to look up.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

All temporal-property keys present in this view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.values">values</strong></td>
<td valign="top">[<a href="#temporalproperty">TemporalProperty</a>!]!</td>
<td>

All temporal properties, each as a `TemporalProperty` with its full timeline
available. Use `history`, `values`, `latest`, `at`, etc. on each entry.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

Optional whitelist. If provided, only temporal properties with these keys are returned; if omitted, every temporal property in the view is returned.

</td>
</tr>
</tbody>
</table>

### TemporalProperty

The full timeline of a single property key on one entity. Exposes every
update (via `values` / `history` / `orderedDedupe`), point lookups (`at`,
`latest`), and aggregates over the timeline (`sum`, `mean`, `min`, `max`,
`median`, `count`).

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The property key (name).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.dtype">dtype</strong></td>
<td valign="top"><a href="#propertytype">PropertyType</a>!</td>
<td>

The property's declared type, for type-directed decoding of stored
values (`values`, `at`, `latest`, `unique`, `min`, `max`, `median`,
`orderedDedupe`). Aggregates (`sum`, `mean`, `average`) may widen.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.history">history</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td>

Event history for this property — one entry per temporal update, in
insertion order. Use this to navigate the full timeline: access the
raw `timestamps` / `datetimes` / `eventId` lists, analyse gaps between
updates via `intervals` (mean/median/min/max), ask `isEmpty`, or
paginate the events.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.values">values</strong></td>
<td valign="top">[<a href="#propertyoutput">PropertyOutput</a>!]!</td>
<td>

All values this property has ever taken, in temporal order (one per update).
Typed as `PropertyOutput` so numeric values stay numeric.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.at">at</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

The value at or before time `t` (latest update on or before `t`). Returns null
if no update exists on or before `t`.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

A TimeInput (epoch millis integer, RFC3339 string, or `{timestamp, eventId}` object).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.latest">latest</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

The most recent value, or null if the property has never been set in this view.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.unique">unique</strong></td>
<td valign="top">[<a href="#propertyoutput">PropertyOutput</a>!]!</td>
<td>

The set of distinct values this property has ever taken (order not guaranteed).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.ordereddedupe">orderedDedupe</strong></td>
<td valign="top">[<a href="#propertytuple">PropertyTuple</a>!]!</td>
<td>

Collapses runs of consecutive-equal updates into a single `(time, value)` pair.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">latestTime</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

If true, each run is represented by its *last* timestamp; if false, by its *first*. Useful for compressing chatter in a timeline.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.sum">sum</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

Sum of all updates. Returns null if the dtype is not additive or the property is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.mean">mean</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

Mean of all updates as an F64. Returns null if any value is non-numeric or the property is
empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.average">average</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a></td>
<td>

Alias for `mean` — same F64 average, same null cases.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.min">min</strong></td>
<td valign="top"><a href="#propertytuple">PropertyTuple</a></td>
<td>

Minimum `(time, value)` pair. Returns null if the dtype is not comparable or the property is
empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.max">max</strong></td>
<td valign="top"><a href="#propertytuple">PropertyTuple</a></td>
<td>

Maximum `(time, value)` pair. Returns null if the dtype is not comparable or the property is
empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.median">median</strong></td>
<td valign="top"><a href="#propertytuple">PropertyTuple</a></td>
<td>

Median `(time, value)` pair (lower median on even-length inputs). Returns null if the dtype
is not comparable or the property is empty.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of updates recorded for this property in the current view.

</td>
</tr>
</tbody>
</table>

### VectorSelection

A working set of documents / nodes / edges built up via similarity
searches on a `VectorisedGraph`. Selections are mutable: you can grow
them with more hops (`expand*`), dereference the contents (`nodes`,
`edges`, `getDocuments`), or start fresh with `emptySelection`.

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.nodes">nodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Returns a list of nodes in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.edges">edges</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Returns a list of edges in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.getdocuments">getDocuments</strong></td>
<td valign="top">[<a href="#document">Document</a>!]!</td>
<td>

Returns a list of documents in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.addnodes">addNodes</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Add every document associated with the named nodes to the selection.
Documents added this way receive a score of 0 (no similarity ranking).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]!</td>
<td>

Node ids whose documents to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.addedges">addEdges</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Add every document associated with the named edges to the selection.
Documents added this way receive a score of 0 (no similarity ranking).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">edges</td>
<td valign="top">[<a href="#inputedge">InputEdge</a>!]!</td>
<td>

List of `{src, dst}` pairs identifying the edges.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expand">expand</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Grow the selection by including documents that are within `hops` of any
document already in the selection. Two documents are 1 hop apart if
they're on the same entity or on a connected node/edge pair.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">hops</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Number of expansion rounds (1 = direct neighbours).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict expansion to entities active in that interval.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandentitiesbysimilarity">expandEntitiesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Iteratively expand the selection by similarity to a natural-language
query. Each pass takes the one-hop neighbour set of the current
selection and adds the highest-scoring entities (mixed nodes and
edges); the loop continues until `limit` entities have been added.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Total number of entities to add across all passes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to entities active in that interval.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandnodesbysimilarity">expandNodesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Like `expandEntitiesBySimilarity` but restricted to nodes — iteratively
add the highest-scoring adjacent nodes to the selection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Total number of nodes to add across all passes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to nodes active in that interval.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandedgesbysimilarity">expandEdgesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Like `expandEntitiesBySimilarity` but restricted to edges — iteratively
add the highest-scoring adjacent edges to the selection.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Total number of edges to add across all passes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to edges active in that interval.

</td>
</tr>
</tbody>
</table>

### VectorisedGraph

A graph with embedded vector representations for its nodes and edges.
Exposes similarity search over documents, nodes, and edges, plus
selection building (`emptySelection`) and index maintenance
(`optimizeIndex`).

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.optimizeindex">optimizeIndex</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Rebuild (or incrementally update) the on-disk vector indexes for nodes
and edges so subsequent similarity searches hit the fresh embeddings.
Safe to call repeatedly; returns true on success.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.emptyselection">emptySelection</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Returns an empty selection of documents.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.entitiesbysimilarity">entitiesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Find the highest-scoring nodes *and* edges (mixed) by similarity to a
natural-language query. The query is embedded server-side and matched
against indexed entity vectors.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of results to return.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to entities active in that interval.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.nodesbysimilarity">nodesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Find the highest-scoring nodes by similarity to a natural-language
query. The query is embedded server-side and matched against indexed
node vectors.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of nodes to return.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to nodes active in that interval.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.edgesbysimilarity">edgesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Find the highest-scoring edges by similarity to a natural-language
query. The query is embedded server-side and matched against indexed
edge vectors.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Natural-language search string; embedded by the server.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum number of edges to return.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td>

Optional `{start, end}` to restrict matches to edges active in that interval.

</td>
</tr>
</tbody>
</table>

## Inputs

### DegreeFilterNew

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="degreefilternew.direction">direction</strong></td>
<td valign="top"><a href="#degreedirection">DegreeDirection</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="degreefilternew.where">where</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### EdgeAddition

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.src">src</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id (string or non-negative integer).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.dst">dst</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id (string or non-negative integer).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#temporalpropertyinput">TemporalPropertyInput</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### EdgeFilter

GraphQL input type for filtering edges.

`EdgeFilter` represents a composable boolean expression evaluated
against edges in a graph. Filters can target:

- edge **endpoints** (source / destination nodes),
- edge **properties** and **metadata**,
- **temporal scope** (windows, snapshots, latest),
- **layer membership**,
- and **structural edge state** (active, valid, deleted, self-loop).

Filters can be combined recursively using logical operators
(`And`, `Or`, `Not`).

Examples (GraphQL):
```graphql
{
edges(filter: {
And: [
{ IsActive: true },
{ Property: { name: "weight", gt: 0.5 } }
]
}) {
src
dst
}
}
```

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.src">src</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Applies a filter to the **source node** of the edge.

The nested `NodeFilter` is evaluated against the source endpoint.

Example:
`{ Src: { Name: { contains: "alice" } } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.dst">dst</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Applies a filter to the **destination node** of the edge.

The nested `NodeFilter` is evaluated against the destination endpoint.

Example:
`{ Dst: { Id: { eq: 42 } } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters an edge **property** by name and value.

Applies to static or temporal properties depending on context.

Example:
`{ Property: { name: "weight", gt: 0.5 } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters an edge **metadata field**.

Metadata is shared across all temporal versions of an edge.

Example:
`{ Metadata: { name: "source", eq: "imported" } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters a **temporal edge property**.

Used when the property value varies over time and must be
evaluated within a temporal context.

Example:
`{ TemporalProperty: { name: "status", eq: "active" } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.and">and</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td>

Logical **AND** over multiple edge filters.

All nested filters must evaluate to `true`.

Example:
`{ And: [ { IsActive: true }, { IsValid: true } ] }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.or">or</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td>

Logical **OR** over multiple edge filters.

At least one nested filter must evaluate to `true`.

Example:
`{ Or: [ { IsDeleted: true }, { IsSelfLoop: true } ] }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.not">not</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Logical **NOT** over a nested edge filter.

Negates the result of the wrapped filter.

Example:
`{ Not: { IsDeleted: true } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.window">window</strong></td>
<td valign="top"><a href="#edgewindowexpr">EdgeWindowExpr</a></td>
<td>

Restricts edge evaluation to a **time window**.

The window is inclusive of `start` and exclusive of `end`.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.at">at</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td>

Restricts edge evaluation to a **single point in time**.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.before">before</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td>

Restricts edge evaluation to times **strictly before** a given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.after">after</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td>

Restricts edge evaluation to times **strictly after** a given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.latest">latest</strong></td>
<td valign="top"><a href="#edgeunaryexpr">EdgeUnaryExpr</a></td>
<td>

Evaluates edge predicates against the **latest available state**.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td>

Evaluates edge predicates against a **snapshot** of the graph
at a specific time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edgeunaryexpr">EdgeUnaryExpr</a></td>
<td>

Evaluates edge predicates against the **most recent snapshot**
of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.layers">layers</strong></td>
<td valign="top"><a href="#edgelayersexpr">EdgeLayersExpr</a></td>
<td>

Restricts evaluation to edges belonging to one or more **layers**.

Example:
`{ Layers: { values: ["fire_nation", "air_nomads"] } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches edges that have at least one event in the current view/window.

When `true`, only active edges are matched.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isvalid">isValid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches edges that are structurally valid (i.e. not deleted)
in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isdeleted">isDeleted</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches edges that have been deleted in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isselfloop">isSelfLoop</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches edges that are **self-loops**
(source node == destination node).

</td>
</tr>
</tbody>
</table>

### EdgeLayersExpr

Restricts edge evaluation to one or more layers and applies a nested `EdgeFilter`.

Used by `GqlEdgeFilter::Layers`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgelayersexpr.names">names</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgelayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td>

Filter evaluated within the layer-restricted view.

</td>
</tr>
</tbody>
</table>

### EdgeSortBy

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.reverse">reverse</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Reverse order. Applies to the `time` / `property` keys; the node keys
(`src` / `dst` / `neighbour`) carry their own `reverse` inside the
nested `NodeSortBy` and ignore this flag.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.src">src</strong></td>
<td valign="top"><a href="#nodesortby">NodeSortBy</a></td>
<td>

Sort by the source node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.dst">dst</strong></td>
<td valign="top"><a href="#nodesortby">NodeSortBy</a></td>
<td>

Sort by the destination node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.neighbour">neighbour</strong></td>
<td valign="top"><a href="#nodesortby">NodeSortBy</a></td>
<td>

Sort by the neighbour node: the endpoint that is NOT the node these
edges were traversed from (the destination for a graph-level edge
collection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.time">time</strong></td>
<td valign="top"><a href="#sortbytime">SortByTime</a></td>
<td>

Time

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.property">property</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Property

</td>
</tr>
</tbody>
</table>

### EdgeTimeExpr

Restricts edge evaluation to a single time bound and applies a nested `EdgeFilter`.

Used by `At`, `Before`, and `After` edge filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgetimeexpr.time">time</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Reference time for the operation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgetimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td>

Filter evaluated within the restricted time scope.

</td>
</tr>
</tbody>
</table>

### EdgeUnaryExpr

Applies a unary edge-view operation and then evaluates a nested `EdgeFilter`.

Used by `Latest` and `SnapshotLatest` edge filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgeunaryexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td>

Filter evaluated after applying the unary operation.

</td>
</tr>
</tbody>
</table>

### EdgeViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Contains only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Snapshot at latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Snapshot at specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of included layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single excluded layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Edge filter

</td>
</tr>
</tbody>
</table>

### EdgeWindowExpr

Restricts edge evaluation to a time window and applies a nested `EdgeFilter`.

Used by `GqlEdgeFilter::Window`.

The window is inclusive of `start` and exclusive of `end`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowexpr.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window start time (inclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td>

Filter evaluated within the restricted window.

</td>
</tr>
</tbody>
</table>

### EdgesViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Contains only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Snapshot at latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Snapshot at specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of included layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single excluded layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Edge filter

</td>
</tr>
</tbody>
</table>

### EmbeddingModel

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="embeddingmodel.openai">openAI</strong></td>
<td valign="top"><a href="#openaiconfig">OpenAIConfig</a></td>
<td>

OpenAI embedding models or compatible providers

</td>
</tr>
</tbody>
</table>

### ExplodedEdgeFilter

GraphQL input type for filtering **exploded edges** — edge views where each
temporal event is an individually addressable edge instance, rather than
one aggregated edge across time.

Predicates are evaluated **per event**: a property condition keeps the
individual updates that match it (and the edges carrying them), where the
plain `EdgeFilter` evaluates one aggregated value per edge.

Filters can target edge endpoints, properties/metadata, temporal scope,
layer membership, and structural edge state, and can be combined
recursively with `And`/`Or`/`Not` — mirroring `EdgeFilter`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.src">src</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Applies a filter to the **source node** of the exploded edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.dst">dst</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Applies a filter to the **destination node** of the exploded edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters an exploded-edge **property** by name and value, evaluated
per event.

Example:
`{ Property: { name: "weight", gt: 0.5 } }`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters an exploded-edge **metadata field**.

Metadata is shared across all temporal versions of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters a **temporal exploded-edge property**, evaluated within a
temporal context per event.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.and">and</strong></td>
<td valign="top">[<a href="#explodededgefilter">ExplodedEdgeFilter</a>!]</td>
<td>

Logical **AND** over multiple exploded-edge filters.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.or">or</strong></td>
<td valign="top">[<a href="#explodededgefilter">ExplodedEdgeFilter</a>!]</td>
<td>

Logical **OR** over multiple exploded-edge filters.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.not">not</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a></td>
<td>

Logical **NOT** over a nested exploded-edge filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.window">window</strong></td>
<td valign="top"><a href="#explodededgewindowexpr">ExplodedEdgeWindowExpr</a></td>
<td>

Restricts exploded-edge evaluation to a **time window**
(inclusive start, exclusive end).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.at">at</strong></td>
<td valign="top"><a href="#explodededgetimeexpr">ExplodedEdgeTimeExpr</a></td>
<td>

Restricts exploded-edge evaluation to a **single point in time**.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.before">before</strong></td>
<td valign="top"><a href="#explodededgetimeexpr">ExplodedEdgeTimeExpr</a></td>
<td>

Restricts exploded-edge evaluation to times **strictly before** a
given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.after">after</strong></td>
<td valign="top"><a href="#explodededgetimeexpr">ExplodedEdgeTimeExpr</a></td>
<td>

Restricts exploded-edge evaluation to times **strictly after** a
given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.latest">latest</strong></td>
<td valign="top"><a href="#explodededgeunaryexpr">ExplodedEdgeUnaryExpr</a></td>
<td>

Evaluates exploded-edge predicates against the **latest available
state**.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#explodededgetimeexpr">ExplodedEdgeTimeExpr</a></td>
<td>

Evaluates exploded-edge predicates against a **snapshot** of the graph
at a specific time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#explodededgeunaryexpr">ExplodedEdgeUnaryExpr</a></td>
<td>

Evaluates exploded-edge predicates against the **most recent
snapshot** of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.layers">layers</strong></td>
<td valign="top"><a href="#explodededgelayersexpr">ExplodedEdgeLayersExpr</a></td>
<td>

Restricts evaluation to exploded edges belonging to one or more
**layers**.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches exploded edges that have at least one event in the current
view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.isvalid">isValid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches exploded edges that are structurally valid (i.e. not deleted)
in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.isdeleted">isDeleted</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches exploded edges that have been deleted in the current
view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgefilter.isselfloop">isSelfLoop</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches exploded edges that are **self-loops**
(source node == destination node).

</td>
</tr>
</tbody>
</table>

### ExplodedEdgeLayersExpr

Restricts exploded-edge evaluation to one or more layers and applies a
nested `ExplodedEdgeFilter`.

Used by `GqlExplodedEdgeFilter::Layers`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="explodededgelayersexpr.names">names</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgelayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a>!</td>
<td>

Filter evaluated within the layer-restricted view.

</td>
</tr>
</tbody>
</table>

### ExplodedEdgeTimeExpr

Restricts exploded-edge evaluation to a single time bound and applies a
nested `ExplodedEdgeFilter`.

Used by `At`, `Before`, `After`, and `SnapshotAt` exploded-edge filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="explodededgetimeexpr.time">time</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Reference time for the operation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgetimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a>!</td>
<td>

Filter evaluated within the restricted time scope.

</td>
</tr>
</tbody>
</table>

### ExplodedEdgeUnaryExpr

Applies a unary edge-view operation and then evaluates a nested
`ExplodedEdgeFilter`.

Used by `Latest` and `SnapshotLatest` exploded-edge filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="explodededgeunaryexpr.expr">expr</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a>!</td>
<td>

Filter evaluated after applying the unary operation.

</td>
</tr>
</tbody>
</table>

### ExplodedEdgeWindowExpr

Restricts exploded-edge evaluation to a time window and applies a nested
`ExplodedEdgeFilter`.

Used by `GqlExplodedEdgeFilter::Window`.

The window is inclusive of `start` and exclusive of `end`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="explodededgewindowexpr.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window start time (inclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgewindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="explodededgewindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a>!</td>
<td>

Filter evaluated within the restricted window.

</td>
</tr>
</tbody>
</table>

### FuzzySearchExpr

Fuzzy string match: passes when the candidate is within `levenshteinDistance`
edits of `value` (optionally also matching by prefix). Mirrors the local
`fuzzy_search(value, levenshtein_distance, prefix_match)` builder.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="fuzzysearchexpr.value">value</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

The string to match against.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="fuzzysearchexpr.levenshteindistance">levenshteinDistance</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Maximum Levenshtein edit distance for a match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="fuzzysearchexpr.prefixmatch">prefixMatch</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Whether a prefix match within the distance also passes.

</td>
</tr>
</tbody>
</table>

### GqlFilter

A general filter expression — a node filter (`nodes`), an edge filter (`edges`), a graph/view
filter (`graph`, e.g. a layer or window restriction), or an `and`/`or` combination of these
(which may mix kinds). Used where an operation accepts any filter, such as scoping a component
walk.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.nodes">nodes</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Filter by node properties, fields, or temporal state.
(Persisted filters may use the legacy `node` key.)

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.edges">edges</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Filter by edge properties, source/destination, or temporal state.
(Persisted filters may use the legacy `edge` key.)

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.explodededges">explodedEdges</strong></td>
<td valign="top"><a href="#explodededgefilter">ExplodedEdgeFilter</a></td>
<td>

Filter exploded edges — per-event edge instances — by properties,
endpoints, or temporal state, evaluated per event.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.graph">graph</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td>

Apply a graph-level view (window, snapshot, layer restriction, …).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.and">and</strong></td>
<td valign="top">[<a href="#gqlfilter">GqlFilter</a>!]</td>
<td>

All sub-filters must pass (intersection).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.or">or</strong></td>
<td valign="top">[<a href="#gqlfilter">GqlFilter</a>!]</td>
<td>

At least one sub-filter must pass (union).
Cross-type sub-filters (e.g. `nodes` and `edges` together) produce a
proper graph union: a node is visible if it matches the node filter or
has a visible edge, and an edge is visible if it matches the edge
filter or both its endpoints are visible.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.not">not</strong></td>
<td valign="top"><a href="#gqlfilter">GqlFilter</a></td>
<td>

Inverts the nested filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.window">window</strong></td>
<td valign="top"><a href="#graphwindowexpr">GraphWindowExpr</a></td>
<td>

Restrict evaluation to a time window (inclusive start, exclusive end).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.at">at</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to a single point in time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.before">before</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to times strictly before the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.after">after</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to times strictly after the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.latest">latest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td>

Evaluate against the latest available state.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Evaluate against a snapshot of the graph at a given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td>

Evaluate against the most recent snapshot of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlfilter.layers">layers</strong></td>
<td valign="top"><a href="#graphlayersexpr">GraphLayersExpr</a></td>
<td>

Restrict evaluation to one or more layers.

</td>
</tr>
</tbody>
</table>

### GraphFilter

GraphQL input type for restricting a graph view.

`GraphFilter` controls the **evaluation scope** for subsequent node/edge filters:
- time windows (`Window`)
- time points (`At`)
- open-ended ranges (`Before`, `After`)
- latest evaluation (`Latest`)
- snapshots (`SnapshotAt`, `SnapshotLatest`)
- layer membership (`Layers`)

These filters can be nested via the `expr` field on the corresponding
`*Expr` input objects to form pipelines.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.window">window</strong></td>
<td valign="top"><a href="#graphwindowexpr">GraphWindowExpr</a></td>
<td>

Restrict evaluation to a time window (inclusive start, exclusive end).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.at">at</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to a single point in time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.before">before</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to times strictly before the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.after">after</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Restrict evaluation to times strictly after the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.latest">latest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td>

Evaluate against the latest available state.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td>

Evaluate against a snapshot of the graph at a given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td>

Evaluate against the most recent snapshot of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.layers">layers</strong></td>
<td valign="top"><a href="#graphlayersexpr">GraphLayersExpr</a></td>
<td>

Restrict evaluation to one or more layers.

</td>
</tr>
</tbody>
</table>

### GraphLayersExpr

Graph view restriction by layer membership, optionally chaining another `GraphFilter`.

Used by `GqlGraphFilter::Layers`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphlayersexpr.names">names</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphlayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td>

Optional nested filter applied after the layer restriction.

</td>
</tr>
</tbody>
</table>

### GraphTimeExpr

Graph view restriction to a single time bound, optionally chaining another `GraphFilter`.

Used by `At`, `Before`, and `After` graph filters.

Example:
`{ At: { time: 5, expr: { Layers: { names: ["L1"] } } } }`

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphtimeexpr.time">time</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Reference time for the operation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphtimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td>

Optional nested filter applied after the time restriction.

</td>
</tr>
</tbody>
</table>

### GraphUnaryExpr

Graph view restriction that takes only a nested expression.

Used for unary view operations like `Latest` and `SnapshotLatest`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphunaryexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td>

Optional nested filter applied after the unary operation.

</td>
</tr>
</tbody>
</table>

### GraphViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Contains only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of included layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single excluded layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.subgraph">subgraph</strong></td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]</td>
<td>

Subgraph nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.subgraphnodetypes">subgraphNodeTypes</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

Subgraph node types.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludenodes">excludeNodes</strong></td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]</td>
<td>

List of excluded nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.valid">valid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Valid state.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

View at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Snapshot at specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Snapshot at latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

Edge filter.

</td>
</tr>
</tbody>
</table>

### GraphWindowExpr

Graph view restriction to a time window, optionally chaining another `GraphFilter`.

Used by `GqlGraphFilter::Window`.

- `start` and `end` define the window (inclusive start, exclusive end).
- `expr` optionally nests another graph filter to apply *within* this window.

Example (GraphQL):
```graphql
{ Window: { start: 0, end: 10, expr: { Layers: { names: ["A"] } } } }
```

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowexpr.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window start time (inclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td>

Optional nested filter applied after the window restriction.

</td>
</tr>
</tbody>
</table>

### InputEdge

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="inputedge.src">src</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Source node id (string or non-negative integer).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="inputedge.dst">dst</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Destination node id (string or non-negative integer).

</td>
</tr>
</tbody>
</table>

### NodeAddition

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.name">name</strong></td>
<td valign="top"><a href="#nodeid">NodeId</a>!</td>
<td>

Node id (string or non-negative integer).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.nodetype">nodeType</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Node type.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#temporalpropertyinput">TemporalPropertyInput</a>!]</td>
<td>

Updates.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Layer.

</td>
</tr>
</tbody>
</table>

### NodeFieldCondition

Boolean expression over a built-in node field (ID, name, or type).

This is used by `NodeFieldFilterNew.where_` when filtering a specific
`NodeField`.

Supports comparisons, string predicates, and set membership.
(Presence checks and aggregations are handled via property filters instead.)

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.eq">eq</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Equality.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.ne">ne</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Inequality.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.gt">gt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Greater-than.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.ge">ge</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Greater-than-or-equal.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.lt">lt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Less-than.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.le">le</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Less-than-or-equal.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.startswith">startsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

String prefix match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.endswith">endsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

String suffix match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.contains">contains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Substring match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.notcontains">notContains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Negated substring match.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.fuzzysearch">fuzzySearch</strong></td>
<td valign="top"><a href="#fuzzysearchexpr">FuzzySearchExpr</a></td>
<td>

Fuzzy string match (Levenshtein distance, optional prefix matching).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.isin">isIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Set membership.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.isnotin">isNotIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Negated set membership.

</td>
</tr>
</tbody>
</table>

### NodeFieldFilterNew

Filters a built-in node field (`id`, `name`, `type`) using a `NodeFieldCondition`.

Example (GraphQL):
```graphql
{ Node: { field: NodeName, where: { Contains: "ali" } } }
```

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldfilternew.field">field</strong></td>
<td valign="top"><a href="#nodefield">NodeField</a>!</td>
<td>

Which built-in field to filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldfilternew.where">where</strong></td>
<td valign="top"><a href="#nodefieldcondition">NodeFieldCondition</a>!</td>
<td>

Condition applied to the selected field.

Exposed as `where` in GraphQL.

</td>
</tr>
</tbody>
</table>

### NodeFilter

GraphQL input type for filtering nodes.

`NodeFilter` represents a composable boolean expression evaluated
against nodes in a graph. Filters can target:

- built-in node fields (`Node` / `NodeFieldFilterNew`),
- node properties and metadata,
- temporal properties,
- temporal scope (windows, snapshots, latest),
- and layer membership,
- plus node state predicates (e.g. `IsActive`).

Filters can be combined recursively using logical operators
(`And`, `Or`, `Not`).

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.node">node</strong></td>
<td valign="top"><a href="#nodefieldfilternew">NodeFieldFilterNew</a></td>
<td>

Filters a built-in node field (ID, name, or type).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters a node property by name and condition.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.degree">degree</strong></td>
<td valign="top"><a href="#degreefilternew">DegreeFilterNew</a></td>
<td>

Filters a node's degree (in, out, or total) by a condition.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters a node metadata field by name and condition.

Metadata is shared across all temporal versions of a node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Filters a temporal node property by name and condition.

Used when the property value varies over time and must be evaluated
within a temporal context.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.and">and</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td>

Logical AND over multiple node filters.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.or">or</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td>

Logical OR over multiple node filters.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.not">not</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Logical NOT over a nested node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.window">window</strong></td>
<td valign="top"><a href="#nodewindowexpr">NodeWindowExpr</a></td>
<td>

Restricts evaluation to a time window (inclusive start, exclusive end).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.at">at</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td>

Restricts evaluation to a single point in time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.before">before</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td>

Restricts evaluation to times strictly before the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.after">after</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td>

Restricts evaluation to times strictly after the given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.latest">latest</strong></td>
<td valign="top"><a href="#nodeunaryexpr">NodeUnaryExpr</a></td>
<td>

Evaluates predicates against the latest available node state.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td>

Evaluates predicates against a snapshot of the graph at a given time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#nodeunaryexpr">NodeUnaryExpr</a></td>
<td>

Evaluates predicates against the most recent snapshot of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.layers">layers</strong></td>
<td valign="top"><a href="#nodelayersexpr">NodeLayersExpr</a></td>
<td>

Restricts evaluation to nodes belonging to one or more layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Matches nodes that have at least one event in the current view/window.

When `true`, only active nodes are matched.

</td>
</tr>
</tbody>
</table>

### NodeLayersExpr

Restricts node evaluation to one or more layers and applies a nested `NodeFilter`.

Used by `GqlNodeFilter::Layers`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodelayersexpr.names">names</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Layer names to include.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodelayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td>

Filter evaluated within the layer-restricted view.

</td>
</tr>
</tbody>
</table>

### NodeSortBy

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.reverse">reverse</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Reverse order

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.id">id</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Unique Id

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.name">name</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Node name

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.type">type</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Node type. Untyped nodes sort first (before any named type).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.time">time</strong></td>
<td valign="top"><a href="#sortbytime">SortByTime</a></td>
<td>

Time

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.property">property</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Property

</td>
</tr>
</tbody>
</table>

### NodeTimeExpr

Restricts node evaluation to a single time bound and applies a nested `NodeFilter`.

Used by `At`, `Before`, and `After` node filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodetimeexpr.time">time</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Reference time for the operation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodetimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td>

Filter evaluated within the restricted time scope.

</td>
</tr>
</tbody>
</table>

### NodeUnaryExpr

Applies a unary node-view operation and then evaluates a nested `NodeFilter`.

Used by `Latest` and `SnapshotLatest` node filters.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodeunaryexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td>

Filter evaluated after applying the unary operation.

</td>
</tr>
</tbody>
</table>

### NodeViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Contains only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

View at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Snapshot at latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Snapshot at specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of included layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single excluded layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Node filter.

</td>
</tr>
</tbody>
</table>

### NodeWindowExpr

Restricts node evaluation to a time window and applies a nested `NodeFilter`.

Used by `GqlNodeFilter::Window`.

The window is inclusive of `start` and exclusive of `end`.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowexpr.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window start time (inclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window end time (exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td>

Filter evaluated within the restricted window.

</td>
</tr>
</tbody>
</table>

### NodesViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Contains only the default layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

View at the latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Snapshot at latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of included layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single excluded layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Snapshot at specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.typefilter">typeFilter</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of types.

</td>
</tr>
</tbody>
</table>

### ObjectEntry

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="objectentry.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Key.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="objectentry.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
<td>

Value.

</td>
</tr>
</tbody>
</table>

### OpenAIConfig

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="openaiconfig.model">model</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="openaiconfig.apibase">apiBase</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="openaiconfig.apikeyenv">apiKeyEnv</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="openaiconfig.orgid">orgId</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="openaiconfig.projectid">projectId</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
</tbody>
</table>

### PathFromNodeViewCollection

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Latest time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Latest snapshot.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td>

List of excluded layers.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Single layer to exclude.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Window between a start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.at">at</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View at a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.before">before</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View before a specified time (end exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.after">after</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

View after a specified time (start exclusive).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td>

Shrink a Window to a specified start and end time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window start to a specified time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a></td>
<td>

Set the window end to a specified time.

</td>
</tr>
</tbody>
</table>

### PropCondition

Boolean expression over a property value.

`PropCondition` is used inside `PropertyFilterNew.where` to describe
how a property’s value should be matched.

It supports:
- comparisons (`Eq`, `Gt`, `Le`, …),
- string predicates (`Contains`, `StartsWith`, …),
- set membership (`IsIn`, `IsNotIn`),
- presence checks (`IsSome`, `IsNone`),
- boolean composition (`And`, `Or`, `Not`),
- and list/aggregate qualifiers (`First`, `Sum`, `Len`, …).

Notes:
- `Value` is interpreted according to the property’s type.
- Aggregators/qualifiers like `Sum` and `Len` apply when the underlying
property is list-like or aggregatable (depending on your engine rules).

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.eq">eq</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Equality: property value equals the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.ne">ne</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Inequality: property value does not equal the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.gt">gt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Greater-than: property value is greater than the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.ge">ge</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Greater-than-or-equal: property value is >= the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.lt">lt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Less-than: property value is less than the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.le">le</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Less-than-or-equal: property value is <= the given value.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.startswith">startsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

String prefix match against the property's string representation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.endswith">endsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

String suffix match against the property's string representation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.contains">contains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Substring match against the property's string representation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.notcontains">notContains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Negated substring match against the property's string representation.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.fuzzysearch">fuzzySearch</strong></td>
<td valign="top"><a href="#fuzzysearchexpr">FuzzySearchExpr</a></td>
<td>

Fuzzy string match (Levenshtein distance, optional prefix matching).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isin">isIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Set membership: property value is contained in the given list of values.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isnotin">isNotIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td>

Negated set membership: property value is not contained in the given list of values.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.issome">isSome</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Presence check: property value is present (not null/missing).

When set to `true`, requires the property to exist.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isnone">isNone</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Absence check: property value is missing / null.

When set to `true`, requires the property to be missing.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.and">and</strong></td>
<td valign="top">[<a href="#propcondition">PropCondition</a>!]</td>
<td>

Logical AND over nested conditions.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.or">or</strong></td>
<td valign="top">[<a href="#propcondition">PropCondition</a>!]</td>
<td>

Logical OR over nested conditions.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.not">not</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Logical NOT over a nested condition.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.first">first</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **first** element of a list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.last">last</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **last** element of a list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.any">any</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Requires that **any** element of a list-like property matches the nested condition.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.all">all</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Requires that **all** elements of a list-like property match the nested condition.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.sum">sum</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **sum** of a numeric list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.avg">avg</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **average** of a numeric list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.min">min</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **minimum** element of a list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.max">max</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **maximum** element of a list-like property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.len">len</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td>

Applies the nested condition to the **length** of a list-like property.

</td>
</tr>
</tbody>
</table>

### PropertyFilterNew

Filters an entity property or metadata field by name and condition.

This input is used by both node and edge filters when targeting
a specific property key (or metadata key) and applying a `PropCondition`.

Fields:
- `name`: The property key to query.
- `where_`: The condition to apply to that property’s value.

Example (GraphQL):
```graphql
{ Property: { name: "weight", where: { Gt: 0.5 } } }
```

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="propertyfilternew.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Property (or metadata) key.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyfilternew.where">where</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a>!</td>
<td>

Condition applied to the property value.

Exposed as `where` in GraphQL.

</td>
</tr>
</tbody>
</table>

### PropertyInput

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="propertyinput.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Key.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyinput.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
<td>

Value.

</td>
</tr>
</tbody>
</table>

### Seeds

How the initially infected nodes are chosen.

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="seeds.nodes">nodes</strong></td>
<td valign="top">[<a href="#nodeid">NodeId</a>!]</td>
<td>

Infect exactly these nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="seeds.number">number</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Infect this many randomly chosen nodes.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="seeds.probability">probability</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Infect this fraction of the nodes, chosen at random.

</td>
</tr>
</tbody>
</table>

### Template

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="template.enabled">enabled</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

The default template.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="template.custom">custom</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

A custom template.

</td>
</tr>
</tbody>
</table>

### TemporalPropertyInput

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyinput.time">time</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Time of the update — accepts the same forms as `TimeInput` (epoch
millis Int, RFC3339 string, or `{timestamp, eventId}` object).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyinput.properties">properties</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td>

Properties.

</td>
</tr>
</tbody>
</table>

### Value

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="value.u8">u8</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

8 bit unsigned integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.u16">u16</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

16 bit unsigned integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.u32">u32</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

32 bit unsigned integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.u64">u64</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

64 bit unsigned integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.i32">i32</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

32 bit signed integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.i64">i64</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

64 bit signed integer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.f32">f32</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

32 bit float.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.f64">f64</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

64 bit float.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.f32special">f32Special</strong></td>
<td valign="top"><a href="#specialfloat">SpecialFloat</a></td>
<td>

Non-finite 32 bit float (NaN, ±Infinity) — JSON has no number form for these.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.f64special">f64Special</strong></td>
<td valign="top"><a href="#specialfloat">SpecialFloat</a></td>
<td>

Non-finite 64 bit float (NaN, ±Infinity) — JSON has no number form for these.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.str">str</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

String.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.bool">bool</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Boolean.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.list">list</strong></td>
<td valign="top">[<a href="#value">Value</a>!]</td>
<td>

List.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.object">object</strong></td>
<td valign="top">[<a href="#objectentry">ObjectEntry</a>!]</td>
<td>

Object.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.dtime">dtime</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Timezone-aware datetime.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.ndtime">ndtime</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Naive datetime (no timezone).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.decimal">decimal</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

BigDecimal number (string representation, e.g. "3.14159" or "123e-5").

</td>
</tr>
</tbody>
</table>

### VectorisedGraphWindow

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraphwindow.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Inclusive lower bound of the search window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraphwindow.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Exclusive upper bound of the search window.

</td>
</tr>
</tbody>
</table>

### Window

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="window.start">start</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window start time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="window.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td>

Window end time.

</td>
</tr>
</tbody>
</table>

### WindowDuration

<table>
<thead>
<tr>
<th colspan="2" align="left">Field</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong id="windowduration.duration">duration</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Duration of window period.

Choose from:

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="windowduration.epoch">epoch</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Time.

</td>
</tr>
</tbody>
</table>

## Enums

### AlignmentUnit

Alignment unit used to align window boundaries.

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>UNALIGNED</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>MILLISECOND</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>SECOND</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>MINUTE</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>HOUR</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>DAY</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>WEEK</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>MONTH</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>YEAR</strong></td>
<td></td>
</tr>
</tbody>
</table>

### DegreeDirection

Filters nodes by computed degree with a directional scope.

`DegreeFilterNew` lets callers filter on:
- inbound degree (`IN`),
- outbound degree (`OUT`),
- or total degree (`BOTH`).

The selected degree is compared using the `where` condition.

Example (GraphQL):
```graphql
{ Degree: { direction: BOTH, where: { Gt: 10 } } }
```

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>IN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>OUT</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>BOTH</strong></td>
<td></td>
</tr>
</tbody>
</table>

### Direction

Edge direction to follow during traversal.

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>OUT</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>IN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>BOTH</strong></td>
<td></td>
</tr>
</tbody>
</table>

### GraphType

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>PERSISTENT</strong></td>
<td>

Persistent.

</td>
</tr>
<tr>
<td valign="top"><strong>EVENT</strong></td>
<td>

Event.

</td>
</tr>
</tbody>
</table>

### NodeField

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>NODE_ID</strong></td>
<td>

Node ID field.

Represents the graph’s node identifier (numeric or string-backed in the API).

</td>
</tr>
<tr>
<td valign="top"><strong>NODE_NAME</strong></td>
<td>

Node name field.

Represents the human-readable node name (string).

</td>
</tr>
<tr>
<td valign="top"><strong>NODE_TYPE</strong></td>
<td>

Node type field.

Represents the optional node type assigned at node creation (string).

</td>
</tr>
</tbody>
</table>

### SortByTime

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>LATEST</strong></td>
<td>

Latest time

</td>
</tr>
<tr>
<td valign="top"><strong>EARLIEST</strong></td>
<td>

Earliest time

</td>
</tr>
</tbody>
</table>

### SpecialFloat

Non-finite float values, which JSON cannot represent as numbers.

Follows protobuf's JSON mapping convention of spelling these out
explicitly rather than silently coercing them to `null`.

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>NAN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>INFINITY</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>NEG_INFINITY</strong></td>
<td></td>
</tr>
</tbody>
</table>

## Scalars

### Boolean

The `Boolean` scalar type represents `true` or `false`.

### Float

The `Float` scalar type represents signed double-precision fractional values as specified by [IEEE 754](https://en.wikipedia.org/wiki/IEEE_floating_point).

### Int

The `Int` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1.

### NestedIntList

Like [`NestedStringList`] but for integer results (`[[Int]]`) — the
columnar `degree`/`inDegree`/`outDegree` nested terminals.

### NestedStringList

Output-only scalars for the columnar nested terminals below. They carry a
`[[..]]` result (one inner list per source node) in a SINGLE field so the
whole thing is computed in one `blocking_compute`, instead of the
`list { ids }` shape which resolves one `PathFromNode` object — and its own
`blocking_compute` — per source. The derive macro can't register a nested
list type directly, hence the custom scalar.

### NodeId

Identifier for a node — either a string (`"alice"`) or a non-negative
integer (`42`). Use whichever form matches how the graph was indexed
when nodes were added.

### PropertyOutput

### PropertyType

A property's type, as `PropType`'s serde JSON form — round-trippable,
unlike the human-readable string from `getDtypeOf`. Scalars are bare
strings (`"F64"`), containers are tagged objects
(`{"List": "F64"}`, `{"Map": {"a": "I64"}}`, `{"Decimal": {"scale": 2}}`).

### String

The `String` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.

### TimeInput

Input for primary time component. Expects Int, DateTime formatted String, or Object { timestamp, eventId }
where the timestamp is either an Int or a DateTime formatted String, and eventId is a non-negative Int.
Valid string formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%,
%Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.

Internally wraps `InputTime` so write paths (`addNode`, `addEdge`,
`addProperties`, etc.) can preserve auto-increment of `event_id` when only
a timestamp is given. Pass the object form `{timestamp, eventId}` to lock
the event_id explicitly.

### Upload

A multipart file upload


## Unions

### DocumentEntity

Entity associated with document.

<table>
<thead>
<tr>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong><a href="#node">Node</a></strong></td>
<td valign="top">

Raphtory graph node.

</td>
</tr>
<tr>
<td valign="top"><strong><a href="#edge">Edge</a></strong></td>
<td valign="top">

Raphtory graph edge.

</td>
</tr>
</tbody>
</table>

### NamespacedItem

<table>
<thead>
<tr>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong><a href="#namespace">Namespace</a></strong></td>
<td valign="top">

A directory-like container for graphs and nested namespaces. Graphs are
addressed by path (e.g. `"team/project/graph"`), and every segment except
the last is a namespace. Use to browse what's stored on the server without
loading any graph data.

</td>
</tr>
<tr>
<td valign="top"><strong><a href="#metagraph">MetaGraph</a></strong></td>
<td valign="top">

Lightweight summary of a stored graph — its name, path, counts, and
filesystem timestamps — served without deserializing the full graph.
Useful for listing what's available on the server before committing to a
full load.

</td>
</tr>
</tbody>
</table>

### NodeStateValue

A single cell of a node state column: either a plain property value, a
node, or a collection of nodes.

<table>
<thead>
<tr>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong><a href="#nodestateprop">NodeStateProp</a></strong></td>
<td valign="top">

A plain property value of a node state cell.

</td>
</tr>
<tr>
<td valign="top"><strong><a href="#node">Node</a></strong></td>
<td valign="top">

Raphtory graph node.

</td>
</tr>
<tr>
<td valign="top"><strong><a href="#nodes">Nodes</a></strong></td>
<td valign="top">

A lazy collection of nodes from a graph view. Supports all the same view
transforms as `Graph` (window, layer, filter, ...) plus pagination and
sorting. Iterated via `list` / `page` / `ids` / `count`.

</td>
</tr>
</tbody>
</table>

<!-- END graphql-markdown -->

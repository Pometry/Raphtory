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
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.namespaces">namespaces</strong></td>
<td valign="top"><a href="#collectionofnamespace">CollectionOfNamespace</a>!</td>
<td>

Returns root namespaces

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

Encodes graph and returns as string

Returns:: Base64 url safe encoded string

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
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

Returns a plugin.

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
<td></td>
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
<td></td>
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

Move graph from a path path on the server to a new_path on the server.

If namespace is not provided, it will be set to the current working directory.
This applies to both the graph namespace and new graph namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.copygraph">copyGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Copy graph from a path path on the server to a new_path on the server.

If namespace is not provided, it will be set to the current working directory.
This applies to both the graph namespace and new graph namespace.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.uploadgraph">uploadGraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Upload graph file from a path on the client.

Returns::
name of the new graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graph</td>
<td valign="top"><a href="#upload">Upload</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graph</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newPath</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">overwrite</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.createindex">createIndex</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Creates search index.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">indexSpec</td>
<td valign="top"><a href="#indexspecinput">IndexSpecInput</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">inRam</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
</tbody>
</table>

## Objects

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

Fetch one "page" of items, optionally offset by a specified amount.

limit - The size of the page (number of items to fetch).
offset - The number of items to skip (defaults to 0).
page_index - The number of pages (of size limit) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
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

Fetch one "page" of items, optionally offset by a specified amount.

limit - The size of the page (number of items to fetch).
offset - The number of items to skip (defaults to 0).
page_index - The number of pages (of size limit) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
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

Fetch one "page" of items, optionally offset by a specified amount.

limit - The size of the page (number of items to fetch).
offset - The number of items to skip (defaults to 0).
page_index - The number of pages (of size limit) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
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

### Edge

Raphtory graph edge

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

Returns a view of Edge containing all layers in the list of  names .

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of Edge containing all layers except the excluded list of  names .

Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.rolling">rolling</strong></td>
<td valign="top"><a href="#edgewindowset">EdgeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given window duration and optional step using a rolling window.

A rolling window is a window that moves forward by step size at each iteration.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.expanding">expanding</strong></td>
<td valign="top"><a href="#edgewindowset">EdgeWindowSet</a>!</td>
<td>

Creates a WindowSet with the given step size using an expanding window.

An expanding window is a window that grows by step size at each iteration.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.window">window</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events between the specified  start  (inclusive) and  end  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.at">at</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events at a specified  time .

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latest">latest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns the latest time of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events that have not been explicitly deleted at time.

This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events that have not been explicitly deleted at the latest time.

This is equivalent to a no-op for Graph and latest() for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.before">before</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events before a specified  end  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.after">after</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Creates a view of the Edge including all events after a specified  start  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Shrinks both the  start  and  end  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Set the  start  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Set the  end  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.applyviews">applyViews</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Takes a specified selection of views and applies them in order given

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgeviewcollection">EdgeViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the earliest time of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.firstupdate">firstUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latesttime">latestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the latest time of an edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.lastupdate">lastUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.time">time</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the time of an exploded edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the start time for rolling and expanding windows for this edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the latest time that this edge is valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.src">src</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the source node of the edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.dst">dst</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the destination node of the edge.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.nbr">nbr</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Returns the node at the other end of the edge (same as dst() for out-edges and src() for in-edges).

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.id">id</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns the id of the edge.

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

Returns the names of the layer this edge belongs to, assuming it belongs to only one layer.

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
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Returns a list of timestamps of when an edge is added or change to an edge is made.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.deletions">deletions</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td>

Returns a list of timestamps of when an edge is deleted.

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

Checks if the edge is on the same node.

Returns: boolean

</td>
</tr>
</tbody>
</table>

### EdgeSchema

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowset.page">page</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowset.list">list</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### Edges

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

Return a view of Edge containing only the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.layers">layers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a view of Edge containing all layers in the list of  names . Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a view of Edge containing all layers except the excluded list of  names . Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.layer">layer</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a view of Edge containing the specified layer. Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a view of Edge containing all layers except the excluded layer specified. Errors if any of the layers do not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.rolling">rolling</strong></td>
<td valign="top"><a href="#edgeswindowset">EdgesWindowSet</a>!</td>
<td>

Creates a WindowSet with the given window duration and optional step using a rolling window. A rolling window is a window that moves forward by step size at each iteration.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.expanding">expanding</strong></td>
<td valign="top"><a href="#edgeswindowset">EdgesWindowSet</a>!</td>
<td>

Creates a WindowSet with the given step size using an expanding window. An expanding window is a window that grows by step size at each iteration.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.window">window</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events between the specified  start  (inclusive) and  end  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.at">at</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events at a specified  time .

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.latest">latest</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events that have not been explicitly deleted at time. This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events that have not been explicitly deleted at the latest time. This is equivalent to a no-op for Graph and latest() for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.before">before</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events before a specified  end  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.after">after</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Creates a view of the Edge including all events after a specified  start  (exclusive).

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Shrinks both the  start  and  end  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Set the  start  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Set the  end  of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.explode">explode</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns an edge object for each update within the original edge.

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

Specify a sort order.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#edgesortby">EdgeSortBy</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the earliest time that this edges is valid or  None  if the edges is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the latest time the specified edges are valid or  None  if the edges is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.page">page</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.list">list</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### EdgesWindowSet

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeswindowset.page">page</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeswindowset.list">list</strong></td>
<td valign="top">[<a href="#edges">Edges</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlDocument

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
<td colspan="2" valign="top"><strong id="gqldocument.entity">entity</strong></td>
<td valign="top"><a href="#documententity">DocumentEntity</a>!</td>
<td>

Entity associated with document.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqldocument.content">content</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Content of the document.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqldocument.embedding">embedding</strong></td>
<td valign="top">[<a href="#float">Float</a>!]!</td>
<td>

Embedding vector.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqldocument.score">score</strong></td>
<td valign="top"><a href="#float">Float</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlIndexSpec

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
<td colspan="2" valign="top"><strong id="gqlindexspec.nodemetadata">nodeMetadata</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns node metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlindexspec.nodeproperties">nodeProperties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns node properties.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlindexspec.edgemetadata">edgeMetadata</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns edge metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlindexspec.edgeproperties">edgeProperties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns edge properties.

</td>
</tr>
</tbody>
</table>

### GqlVectorSelection

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
<td colspan="2" valign="top"><strong id="gqlvectorselection.nodes">nodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Returns a list of nodes in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.edges">edges</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Returns a list of edges in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.getdocuments">getDocuments</strong></td>
<td valign="top">[<a href="#gqldocument">GqlDocument</a>!]!</td>
<td>

Returns a list of documents in the current selection.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.addnodes">addNodes</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Adds all the documents associated with the specified nodes to the current selection.

Documents added by this call are assumed to have a score of 0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.addedges">addEdges</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Adds all the documents associated with the specified edges to the current selection.

Documents added by this call are assumed to have a score of 0.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">edges</td>
<td valign="top">[<a href="#inputedge">InputEdge</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.expand">expand</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Add all the documents a specified number of hops away to the selection.

Two documents A and B are considered to be 1 hop away of each other if they are on the same entity or if they are on the same node and edge pair.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">hops</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.expandentitiesbysimilarity">expandEntitiesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Adds documents, from the set of one hop neighbours to the current selection, to the selection based on their similarity score with the specified query. This function loops so that the set of one hop neighbours expands on each loop and number of documents added is determined by the specified limit.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.expandnodesbysimilarity">expandNodesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Add the adjacent nodes with higher score for query to the selection up to a specified limit. This function loops like expand_entities_by_similarity but is restricted to nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlvectorselection.expandedgesbysimilarity">expandEdgesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td>

Add the adjacent edges with higher score for query to the selection up to a specified limit. This function loops like expand_entities_by_similarity but is restricted to edges.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
</tbody>
</table>

### Graph

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

Returns the names of all layers in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing only the default edge layer.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.layers">layers</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing all the specified layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing all layers except the specified excluded layers.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.layer">layer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing the layer specified.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing all layers except the specified excluded layer.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.subgraph">subgraph</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a subgraph of a specified set of nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.valid">valid</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view of the graph that only includes valid edges.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.subgraphnodetypes">subgraphNodeTypes</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a subgraph filtered by the specified node types.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.excludenodes">excludeNodes</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a subgraph containing all nodes except the specified excluded nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.rolling">rolling</strong></td>
<td valign="top"><a href="#graphwindowset">GraphWindowSet</a>!</td>
<td>

Creates a rolling window with the specified window size and an optional step..

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.expanding">expanding</strong></td>
<td valign="top"><a href="#graphwindowset">GraphWindowSet</a>!</td>
<td>

Creates a expanding window with the specified step size.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.window">window</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Return a graph containing only the activity between  start  and  end  measured as milliseconds from epoch

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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

Create a view including all events that have not been explicitly deleted at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Create a view including all events that have not been explicitly deleted at the latest time.

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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Shrink both the start and end of the window.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Set the start of the window to the larger of start and self.start().

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Set the end of the window to the smaller of end and self.end()

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.created">created</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the timestamp for the creation of the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.lastopened">lastOpened</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the graph's last opened timestamp.

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
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the timestamp of the earliest activity in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.latesttime">latestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the timestamp of the latest activity in the graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the earliest time that this graph is valid.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the latest time that this graph is valid or None if the graph is valid for all times.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.earliestedgetime">earliestEdgeTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Returns the earliest time that any edge in this graph is valid.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.latestedgetime">latestEdgeTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

/// Returns the latest time that any edge in this graph is valid.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.countedges">countEdges</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges in the graph.

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

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.hasnode">hasNode</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if the graph contains the specified node.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.hasedge">hasEdge</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns true if the graph contains the specified edge. Edges are specified by providing a source and destination node id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.node">node</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td>

Gets the node with the specified id.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.nodes">nodes</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Gets (optionally a subset of) the nodes in the graph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">ids</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
<td>

Gets the edge with the specified source and destination nodes.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Gets the edges in the graph.

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.sharedneighbours">sharedNeighbours</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">selectedNodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.exportto">exportTo</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Export all nodes and edges from this graph view to another existing graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.getindexspec">getIndexSpec</strong></td>
<td valign="top"><a href="#gqlindexspec">GqlIndexSpec</a>!</td>
<td>

Get index specification.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.searchnodes">searchNodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Searches for nodes which match the given filter expression.

Uses Tantivy's exact search.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.searchedges">searchEdges</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td>

Searches for edges which match the given filter expression.

Uses Tantivy's exact search.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.applyviews">applyViews</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#graphviewcollection">GraphViewCollection</a>!]!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowset.page">page</strong></td>
<td valign="top">[<a href="#graph">Graph</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowset.list">list</strong></td>
<td valign="top">[<a href="#graph">Graph</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### LayerSchema

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

### MetaGraph

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.created">created</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.lastopened">lastOpened</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.lastupdated">lastUpdated</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.nodecount">nodeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.edgecount">edgeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metagraph.metadata">metadata</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### Metadata

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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### MutableEdge

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

Use to check if adding the edge was successful

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Get the non-mutable edge for querying

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.src">src</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Get the mutable source node of the edge

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.dst">dst</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Get the mutable destination node of the edge

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.delete">delete</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Mark the edge as deleted at time  time

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to the edge (errors if the value already exists)

If this is called after  add_edge , the layer is inherited from the  add_edge  and does not
need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of the edge (existing values are overwritten)

If this is called after  add_edge , the layer is inherited from the  add_edge  and does not
need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutableedge.addupdates">addUpdates</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add temporal property updates to the edge

If this is called after  add_edge , the layer is inherited from the  add_edge  and does not
need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
</tbody>
</table>

### MutableGraph

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

Get the non-mutable graph

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.node">node</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a></td>
<td>

Get mutable existing node

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addnode">addNode</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Add a new node or add updates to an existing node

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.createnode">createNode</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a>!</td>
<td>

Create a new node or fail if it already exists

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addnodes">addNodes</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add a batch of nodes

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#nodeaddition">NodeAddition</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.edge">edge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a></td>
<td>

Get a mutable existing edge

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addedge">addEdge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a>!</td>
<td>

Add a new edge or add updates to an existing edge

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addedges">addEdges</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add a batch of edges

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">edges</td>
<td valign="top">[<a href="#edgeaddition">EdgeAddition</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.deleteedge">deleteEdge</strong></td>
<td valign="top"><a href="#mutableedge">MutableEdge</a>!</td>
<td>

Mark an edge as deleted (creates the edge if it did not exist)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addproperties">addProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add temporal properties to graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to graph (errors if the property already exists)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of the graph (overwrites existing values)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### MutableNode

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

Use to check if adding the node was successful

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Get the non-mutable  Node

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.addmetadata">addMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add metadata to the node (errors if the property already exists)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.setnodetype">setNodeType</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Set the node type (errors if the node already has a non-default type)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">newType</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.updatemetadata">updateMetadata</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update metadata of the node (overwrites existing property values)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablenode.addupdates">addUpdates</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add temporal property updates to the node

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.parent">parent</strong></td>
<td valign="top"><a href="#namespace">Namespace</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.children">children</strong></td>
<td valign="top"><a href="#collectionofnamespace">CollectionOfNamespace</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.items">items</strong></td>
<td valign="top"><a href="#collectionofnamespaceditem">CollectionOfNamespacedItem</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### Node

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
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.layers">layers</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.layer">layer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.rolling">rolling</strong></td>
<td valign="top"><a href="#nodewindowset">NodeWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.expanding">expanding</strong></td>
<td valign="top"><a href="#nodewindowset">NodeWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.window">window</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.at">at</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.latest">latest</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.before">before</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.after">after</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.firstupdate">firstUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.latesttime">latestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.lastupdate">lastUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.history">history</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.edgehistorycount">edgeHistoryCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.nodetype">nodeType</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.properties">properties</strong></td>
<td valign="top"><a href="#properties">Properties</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.metadata">metadata</strong></td>
<td valign="top"><a href="#metadata">Metadata</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.degree">degree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number of edges connected to this node

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outdegree">outDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number edges with this node as the source

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.indegree">inDegree</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Returns the number edges with this node as the destination

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.incomponent">inComponent</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outcomponent">outComponent</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.edges">edges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outedges">outEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inedges">inEdges</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.neighbours">neighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### NodeSchema

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeschema.properties">properties</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td>

Returns the list of property schemas for this node

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeschema.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyschema">PropertySchema</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### NodeWindowSet

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowset.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowset.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### Nodes

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.layers">layers</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.layer">layer</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.rolling">rolling</strong></td>
<td valign="top"><a href="#nodeswindowset">NodesWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.expanding">expanding</strong></td>
<td valign="top"><a href="#nodeswindowset">NodesWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.window">window</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.at">at</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.latest">latest</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.before">before</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.after">after</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">filter</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.applyviews">applyViews</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#nodesviewcollection">NodesViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.sorted">sorted</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#nodesortby">NodeSortBy</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### NodesWindowSet

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeswindowset.page">page</strong></td>
<td valign="top">[<a href="#nodes">Nodes</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeswindowset.list">list</strong></td>
<td valign="top">[<a href="#nodes">Nodes</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### PagerankOutput

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

### PathFromNode

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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.layer">layer</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.rolling">rolling</strong></td>
<td valign="top"><a href="#pathfromnodewindowset">PathFromNodeWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.expanding">expanding</strong></td>
<td valign="top"><a href="#pathfromnodewindowset">PathFromNodeWindowSet</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.window">window</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.at">at</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.latest">latest</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.before">before</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.after">after</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.applyviews">applyViews</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#pathfromnodeviewcollection">PathFromNodeViewCollection</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### PathFromNodeWindowSet

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodewindowset.page">page</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td>

Fetch one "page" of items, optionally offset by a specified amount.

limit  - The size of the page (number of items to fetch).
offset  - The number of items to skip (defaults to 0).
page_index  - The number of pages (of size  limit ) to skip (defaults to 0).

e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
will be returned.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">offset</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">pageIndex</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodewindowset.list">list</strong></td>
<td valign="top">[<a href="#pathfromnode">PathFromNode</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### Properties

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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.temporal">temporal</strong></td>
<td valign="top"><a href="#temporalproperties">TemporalProperties</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### Property

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="property.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="property.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a>!</td>
<td></td>
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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertytuple.value">value</strong></td>
<td valign="top"><a href="#propertyoutput">PropertyOutput</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.values">values</strong></td>
<td valign="top">[<a href="#temporalproperty">TemporalProperty</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### TemporalProperty

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.history">history</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.values">values</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.at">at</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.latest">latest</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.unique">unique</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.ordereddedupe">orderedDedupe</strong></td>
<td valign="top">[<a href="#propertytuple">PropertyTuple</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">latestTime</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### VectorisedGraph

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
<td colspan="2" valign="top"><strong id="vectorisedgraph.emptyselection">emptySelection</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.entitiesbysimilarity">entitiesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.nodesbysimilarity">nodesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.edgesbysimilarity">edgesBySimilarity</strong></td>
<td valign="top"><a href="#gqlvectorselection">GqlVectorSelection</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">limit</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">window</td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
</tbody>
</table>

## Inputs

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
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.dst">dst</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#temporalpropertyinput">TemporalPropertyInput</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### EdgeFilter

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
<td valign="top"><a href="#nodefieldfilter">NodeFieldFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.dst">dst</strong></td>
<td valign="top"><a href="#nodefieldfilter">NodeFieldFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilterexpr">PropertyFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#metadatafilterexpr">MetadataFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#temporalpropertyfilterexpr">TemporalPropertyFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.and">and</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.or">or</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.not">not</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.src">src</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.dst">dst</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.time">time</strong></td>
<td valign="top"><a href="#sortbytime">SortByTime</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.property">property</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.subgraph">subgraph</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.subgraphnodetypes">subgraphNodeTypes</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.excludenodes">excludeNodes</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.valid">valid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
</tr>
</tbody>
</table>

### IndexSpecInput

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
<td colspan="2" valign="top"><strong id="indexspecinput.nodeprops">nodeProps</strong></td>
<td valign="top"><a href="#propsinput">PropsInput</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="indexspecinput.edgeprops">edgeProps</strong></td>
<td valign="top"><a href="#propsinput">PropsInput</a>!</td>
<td></td>
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
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="inputedge.dst">dst</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### MetadataFilterExpr

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
<td colspan="2" valign="top"><strong id="metadatafilterexpr.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadatafilterexpr.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadatafilterexpr.value">value</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
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
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.nodetype">nodeType</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.metadata">metadata</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#temporalpropertyinput">TemporalPropertyInput</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### NodeFieldFilter

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
<td colspan="2" valign="top"><strong id="nodefieldfilter.field">field</strong></td>
<td valign="top"><a href="#nodefield">NodeField</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldfilter.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldfilter.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### NodeFilter

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
<td valign="top"><a href="#nodefieldfilter">NodeFieldFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilterexpr">PropertyFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#metadatafilterexpr">MetadataFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#temporalpropertyfilterexpr">TemporalPropertyFilterExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.and">and</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.or">or</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.not">not</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.id">id</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.time">time</strong></td>
<td valign="top"><a href="#sortbytime">SortByTime</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesortby.property">property</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.typefilter">typeFilter</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="objectentry.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.layers">layers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.excludelayers">excludeLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.layer">layer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.window">window</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.at">at</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.before">before</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.after">after</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#window">Window</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnodeviewcollection.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
</tbody>
</table>

### PropertyFilterExpr

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
<td colspan="2" valign="top"><strong id="propertyfilterexpr.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyfilterexpr.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyfilterexpr.value">value</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyinput.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### PropsInput

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
<td colspan="2" valign="top"><strong id="propsinput.all">all</strong></td>
<td valign="top"><a href="#allpropertyspec">AllPropertySpec</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propsinput.some">some</strong></td>
<td valign="top"><a href="#somepropertyspec">SomePropertySpec</a></td>
<td></td>
</tr>
</tbody>
</table>

### SomePropertySpec

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
<td colspan="2" valign="top"><strong id="somepropertyspec.metadata">metadata</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="somepropertyspec.properties">properties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### TemporalPropertyFilterExpr

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
<td colspan="2" valign="top"><strong id="temporalpropertyfilterexpr.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyfilterexpr.temporal">temporal</strong></td>
<td valign="top"><a href="#temporaltype">TemporalType</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyfilterexpr.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyfilterexpr.value">value</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalpropertyinput.properties">properties</strong></td>
<td valign="top">[<a href="#propertyinput">PropertyInput</a>!]</td>
<td></td>
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
<td colspan="2" valign="top"><strong id="value.u64">u64</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.i64">i64</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.f64">f64</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.str">str</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.bool">bool</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.list">list</strong></td>
<td valign="top">[<a href="#value">Value</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="value.object">object</strong></td>
<td valign="top">[<a href="#objectentry">ObjectEntry</a>!]</td>
<td></td>
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
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="window.end">end</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="windowduration.epoch">epoch</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
</tbody>
</table>

## Enums

### AllPropertySpec

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>ALL</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>ALL_METADATA</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>ALL_PROPERTIES</strong></td>
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
<td></td>
</tr>
<tr>
<td valign="top"><strong>EVENT</strong></td>
<td></td>
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
<td valign="top"><strong>NODE_NAME</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>NODE_TYPE</strong></td>
<td></td>
</tr>
</tbody>
</table>

### Operator

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>EQUAL</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>NOT_EQUAL</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>GREATER_THAN_OR_EQUAL</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>LESS_THAN_OR_EQUAL</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>GREATER_THAN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>LESS_THAN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>IS_NONE</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>IS_SOME</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>IS_IN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>IS_NOT_IN</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>CONTAINS</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>NOT_CONTAINS</strong></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td valign="top"><strong>EARLIEST</strong></td>
<td></td>
</tr>
</tbody>
</table>

### TemporalType

<table>
<thead>
<tr>
<th align="left">Value</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td valign="top"><strong>ANY</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>LATEST</strong></td>
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

### PropertyOutput

### String

The `String` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.

### Upload


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
<td></td>
</tr>
<tr>
<td valign="top"><strong><a href="#edge">Edge</a></strong></td>
<td valign="top">

Raphtory graph edge

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
<td></td>
</tr>
<tr>
<td valign="top"><strong><a href="#metagraph">MetaGraph</a></strong></td>
<td></td>
</tr>
</tbody>
</table>

<!-- END graphql-markdown -->

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

Encodes graph and returns as string

Returns:: Base64 url safe encoded string

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.version">version</strong></td>
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

Upload a graph file from a path on the client using GQL multipart uploading.

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

(Experimental) Creates search index.

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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latest">latest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Returns a view of the edge at the latest time of the graph.

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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns the id of the edge.

Returns:
list[str]:

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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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

Creates a view of the Edge including all events that are valid at time. This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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

Specify a sort order from: source, destination, property, time. You can also reverse the ordering.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#edgesortby">EdgeSortBy</a>!]!</td>
<td></td>
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
<td>

Returns a list of all objects in the current selection of the collection. You should filter the collection first then call list.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.filter">filter</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns a filtered view that applies to list down the chain

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edges.select">select</strong></td>
<td valign="top"><a href="#edges">Edges</a>!</td>
<td>

Returns filtered list of edges

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

Returns the names of all layers in the graphview.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td>

Returns a view containing only the default layer.

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

Returns a subgraph of a specified set of nodes which contains only the edges that connect nodes of the subgraph to each other.

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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
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
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
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

Create a view including all events that are valid at the specified time.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
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
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td colspan="2" valign="top"><strong id="graph.earliestedgetime">earliestEdgeTime</strong></td>
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
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
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
<td>

Returns the latest time that any edge in this graph is valid.

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

Returns true if the graph contains the specified edge. Edges are specified by providing a source and destination node id. You can restrict the search to a specified layer.

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
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
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
<td colspan="2" align="right" valign="top">select</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td></td>
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
<td colspan="2" valign="top"><strong id="graph.filter">filter</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.filternodes">filterNodes</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.filteredges">filterEdges</strong></td>
<td valign="top"><a href="#graph">Graph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.getindexspec">getIndexSpec</strong></td>
<td valign="top"><a href="#indexspec">IndexSpec</a>!</td>
<td>

(Experimental) Get index specification.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graph.searchnodes">searchNodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td>

(Experimental) Searches for nodes which match the given filter expression.

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

(Experimental) Searches the index for edges which match the given filter expression.

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
<td>

Returns the specified graph view or if none is specified returns the default view.
This allows you to specify multiple operations together.

</td>
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

Returns the number of items.

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
<td colspan="2" valign="top"><strong id="history.isempty">isEmpty</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Returns True if the history is empty.

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
<td></td>
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

Returns an Intervals object which calculates the intervals between consecutive EventTime timestamps.

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
<td></td>
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
<td></td>
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
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
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
<td colspan="2" align="right" valign="top">filterBroken</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
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
</tbody>
</table>

### IndexSpec

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
<td colspan="2" valign="top"><strong id="indexspec.nodemetadata">nodeMetadata</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns node metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="indexspec.nodeproperties">nodeProperties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns node properties.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="indexspec.edgemetadata">edgeMetadata</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns edge metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="indexspec.edgeproperties">edgeProperties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Returns edge properties.

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

</td>
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
<td>

Get metadata value matching the specified key.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

/// Check if the key is in the metadata.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Return all metadata keys.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="metadata.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td>

/// Return all metadata values.

</td>
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

Mark the edge as deleted at time time.

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

Add metadata to the edge (errors if the value already exists).

If this is called after add_edge, the layer is inherited from the add_edge and does not
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

Update metadata of the edge (existing values are overwritten).

If this is called after add_edge, the layer is inherited from the add_edge and does not
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

Add temporal property updates to the edge.

If this is called after add_edge, the layer is inherited from the add_edge and does not
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

Get the non-mutable graph.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutablegraph.node">node</strong></td>
<td valign="top"><a href="#mutablenode">MutableNode</a></td>
<td>

Get mutable existing node.

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

Add a new node or add updates to an existing node.

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

Create a new node or fail if it already exists.

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

Add a batch of nodes.

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

Get a mutable existing edge.

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

Add a new edge or add updates to an existing edge.

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

Add a batch of edges.

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

Mark an edge as deleted (creates the edge if it did not exist).

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

Add temporal properties to graph.

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

Add metadata to graph (errors if the property already exists).

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

Update metadata of the graph (overwrites existing values).

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

Add metadata to the node (errors if the property already exists).

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

Set the node type (errors if the node already has a non-default type).

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

Update metadata of the node (overwrites existing property values).

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

Add temporal property updates to the node.

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
<td valign="top"><a href="#string">String</a>!</td>
<td>

Returns the unique id of the node.

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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td colspan="2" valign="top"><strong id="nodes.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td>

Returns a view of the node ids.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.filter">filter</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Returns a filtered view that applies to list down the chain

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodes.select">select</strong></td>
<td valign="top"><a href="#nodes">Nodes</a>!</td>
<td>

Returns filtered list of nodes

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
<td>

Returns a view of PathFromNode containing the specified layer, errors if the layer does not exist.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">step</td>
<td valign="top"><a href="#windowduration">WindowDuration</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">alignmentUnit</td>
<td valign="top"><a href="#alignmentunit">AlignmentUnit</a></td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
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
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Filter nodes by type.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
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
<td colspan="2" valign="top"><strong id="pathfromnode.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
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
<td>

Returns the node ids.

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.filter">filter</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns a filtered view that applies to list down the chain

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="pathfromnode.select">select</strong></td>
<td valign="top"><a href="#pathfromnode">PathFromNode</a>!</td>
<td>

Returns filtered list of neighbour nodes

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">expr</td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
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

Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
The page_index sets the number of pages to skip (defaults to 0).

For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
<td>

Get property value matching the specified key.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if the key is in the properties.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Return all property keys.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="properties.values">values</strong></td>
<td valign="top">[<a href="#property">Property</a>!]!</td>
<td>

Return all property values.

</td>
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
<td valign="top"><a href="#eventtime">EventTime</a>!</td>
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
<td>

Get property value matching the specified key.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Check if the key is in the properties.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Return all property keys.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperties.values">values</strong></td>
<td valign="top">[<a href="#temporalproperty">TemporalProperty</a>!]!</td>
<td>

Return all property values.

</td>
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
<td>

Key of a property.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.history">history</strong></td>
<td valign="top"><a href="#history">History</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.values">values</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

Return the values of the properties.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="temporalproperty.at">at</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
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

### VectorSelection

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
<td colspan="2" valign="top"><strong id="vectorselection.addedges">addEdges</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
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
<td colspan="2" valign="top"><strong id="vectorselection.expand">expand</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandentitiesbysimilarity">expandEntitiesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandnodesbysimilarity">expandNodesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorselection.expandedgesbysimilarity">expandEdgesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
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
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Returns an empty selection of documents.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.entitiesbysimilarity">entitiesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Search the top scoring entities according to a specified query returning no more than a specified limit of entities.

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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.nodesbysimilarity">nodesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Search the top scoring nodes according to a specified query returning no more than a specified limit of nodes.

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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraph.edgesbysimilarity">edgesBySimilarity</strong></td>
<td valign="top"><a href="#vectorselection">VectorSelection</a>!</td>
<td>

Search the top scoring edges according to a specified query returning no more than a specified limit of edges.

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
<td valign="top"><a href="#vectorisedgraphwindow">VectorisedGraphWindow</a></td>
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
<td>

Source node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.dst">dst</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination node.

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

Source node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.dst">dst</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

Destination node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Property filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Metadata filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Temporal property filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.and">and</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td>

AND operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.or">or</strong></td>
<td valign="top">[<a href="#edgefilter">EdgeFilter</a>!]</td>
<td>

OR operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.not">not</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a></td>
<td>

NOT operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.window">window</strong></td>
<td valign="top"><a href="#edgewindowexpr">EdgeWindowExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.at">at</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.before">before</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.after">after</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.latest">latest</strong></td>
<td valign="top"><a href="#edgeunaryexpr">EdgeUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edgetimeexpr">EdgeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edgeunaryexpr">EdgeUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.layers">layers</strong></td>
<td valign="top"><a href="#edgelayersexpr">EdgeLayersExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Edge is active in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isvalid">isValid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Edge is valid (undeleted) in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isdeleted">isDeleted</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Edge is deleted in the current view/window.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgefilter.isselfloop">isSelfLoop</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Edge is a self-loop in the current view/window.

</td>
</tr>
</tbody>
</table>

### EdgeLayersExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgelayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
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
<td>

Reverse order

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.src">src</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Source node

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesortby.dst">dst</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Destination

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgetimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### EdgeUnaryExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgewindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#edgefilter">EdgeFilter</a>!</td>
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

### GraphFilter

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.at">at</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.before">before</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.after">after</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.latest">latest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#graphtimeexpr">GraphTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#graphunaryexpr">GraphUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphfilter.layers">layers</strong></td>
<td valign="top"><a href="#graphlayersexpr">GraphLayersExpr</a></td>
<td></td>
</tr>
</tbody>
</table>

### GraphLayersExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphlayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td></td>
</tr>
</tbody>
</table>

### GraphTimeExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphtimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
<td></td>
</tr>
</tbody>
</table>

### GraphUnaryExpr

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
<td valign="top">[<a href="#string">String</a>!]</td>
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
<td valign="top">[<a href="#string">String</a>!]</td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphwindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#graphfilter">GraphFilter</a></td>
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
<td>

Node properties.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="indexspecinput.edgeprops">edgeProps</strong></td>
<td valign="top"><a href="#propsinput">PropsInput</a>!</td>
<td>

Edge properties.

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
<td valign="top"><a href="#string">String</a>!</td>
<td>

Source node.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="inputedge.dst">dst</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td>

Destination node.

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
<td valign="top"><a href="#string">String</a>!</td>
<td>

Name.

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
</tbody>
</table>

### NodeFieldCondition

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.ne">ne</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.gt">gt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.ge">ge</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.lt">lt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.le">le</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.startswith">startsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.endswith">endsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.contains">contains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.notcontains">notContains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.isin">isIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldcondition.isnotin">isNotIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
</tbody>
</table>

### NodeFieldFilterNew

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefieldfilternew.where">where</strong></td>
<td valign="top"><a href="#nodefieldcondition">NodeFieldCondition</a>!</td>
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
<td valign="top"><a href="#nodefieldfilternew">NodeFieldFilterNew</a></td>
<td>

Node filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.property">property</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Property filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.metadata">metadata</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Metadata filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.temporalproperty">temporalProperty</strong></td>
<td valign="top"><a href="#propertyfilternew">PropertyFilterNew</a></td>
<td>

Temporal property filter.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.and">and</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td>

AND operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.or">or</strong></td>
<td valign="top">[<a href="#nodefilter">NodeFilter</a>!]</td>
<td>

OR operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.not">not</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a></td>
<td>

NOT operator.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.window">window</strong></td>
<td valign="top"><a href="#nodewindowexpr">NodeWindowExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.at">at</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.before">before</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.after">after</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.latest">latest</strong></td>
<td valign="top"><a href="#nodeunaryexpr">NodeUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#nodetimeexpr">NodeTimeExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#nodeunaryexpr">NodeUnaryExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.layers">layers</strong></td>
<td valign="top"><a href="#nodelayersexpr">NodeLayersExpr</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodefilter.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Node is active in the current view/window.

</td>
</tr>
</tbody>
</table>

### NodeLayersExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodelayersexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodetimeexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### NodeUnaryExpr

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowexpr.end">end</strong></td>
<td valign="top"><a href="#timeinput">TimeInput</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodewindowexpr.expr">expr</strong></td>
<td valign="top"><a href="#nodefilter">NodeFilter</a>!</td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.ne">ne</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.gt">gt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.ge">ge</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.lt">lt</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.le">le</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.startswith">startsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.endswith">endsWith</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.contains">contains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.notcontains">notContains</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isin">isIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isnotin">isNotIn</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.issome">isSome</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.isnone">isNone</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.and">and</strong></td>
<td valign="top">[<a href="#propcondition">PropCondition</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.or">or</strong></td>
<td valign="top">[<a href="#propcondition">PropCondition</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.not">not</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.first">first</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.last">last</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.any">any</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.all">all</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.sum">sum</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.avg">avg</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.min">min</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.max">max</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propcondition.len">len</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a></td>
<td></td>
</tr>
</tbody>
</table>

### PropertyFilterNew

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propertyfilternew.where">where</strong></td>
<td valign="top"><a href="#propcondition">PropCondition</a>!</td>
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
<td>

All properties and metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="propsinput.some">some</strong></td>
<td valign="top"><a href="#somepropertyspec">SomePropertySpec</a></td>
<td>

Some properties and metadata.

</td>
</tr>
</tbody>
</table>

### SomePropertySpec

SomePropertySpec object containing lists of metadata and property names.

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
<td>

List of metadata.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="somepropertyspec.properties">properties</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td>

List of properties.

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
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Time.

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
<td valign="top"><a href="#int">Int</a>!</td>
<td>

Start time.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="vectorisedgraphwindow.end">end</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td>

End time.

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
<td>

All properties and metadata.

</td>
</tr>
<tr>
<td valign="top"><strong>ALL_METADATA</strong></td>
<td>

All metadata.

</td>
</tr>
<tr>
<td valign="top"><strong>ALL_PROPERTIES</strong></td>
<td>

All properties.

</td>
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

Node id.

</td>
</tr>
<tr>
<td valign="top"><strong>NODE_NAME</strong></td>
<td>

Node name.

</td>
</tr>
<tr>
<td valign="top"><strong>NODE_TYPE</strong></td>
<td>

Node type.

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

### TimeInput

Input for primary time component. Expects Int, DateTime formatted String, or Object { timestamp, eventId }
where the timestamp is either an Int or a DateTime formatted String, and eventId is a non-negative Int.
Valid string formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%,
%Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.

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
<td></td>
</tr>
<tr>
<td valign="top"><strong><a href="#metagraph">MetaGraph</a></strong></td>
<td></td>
</tr>
</tbody>
</table>

<!-- END graphql-markdown -->

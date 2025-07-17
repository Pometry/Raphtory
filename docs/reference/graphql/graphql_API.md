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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.graph">graph</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
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
<td valign="top"><a href="#gqlmutablegraph">GqlMutableGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.vectorisedgraph">vectorisedGraph</strong></td>
<td valign="top"><a href="#gqlvectorisedgraph">GqlVectorisedGraph</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.namespaces">namespaces</strong></td>
<td valign="top">[<a href="#namespace">Namespace</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.namespace">namespace</strong></td>
<td valign="top"><a href="#namespace">Namespace</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.root">root</strong></td>
<td valign="top"><a href="#namespace">Namespace</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.graphs">graphs</strong></td>
<td valign="top"><a href="#gqlgraphs">GqlGraphs</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.plugins">plugins</strong></td>
<td valign="top"><a href="#queryplugin">QueryPlugin</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="queryroot.receivegraph">receiveGraph</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.deletegraph">deleteGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.newgraph">newGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">path</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">graphType</td>
<td valign="top"><a href="#gqlgraphtype">GqlGraphType</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="mutroot.movegraph">moveGraph</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
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
<td></td>
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

Use GQL multipart upload to send new graphs to server

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

Send graph bincode as base64 encoded string

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

Create a subgraph out of some existing graph in the server

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
</tbody>
</table>

## Objects

### Document

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
<td valign="top"><a href="#gqldocumententity">GqlDocumentEntity</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.content">content</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.embedding">embedding</strong></td>
<td valign="top">[<a href="#float">Float</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="document.life">life</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### Edge

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
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layers">layers</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layer">layer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.window">window</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
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
<td colspan="2" valign="top"><strong id="edge.at">at</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latest">latest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.before">before</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.after">after</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
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
<td colspan="2" valign="top"><strong id="edge.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.applyviews">applyViews</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgeviewcollection">EdgeViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.firstupdate">firstUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.latesttime">latestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.lastupdate">lastUpdate</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.time">time</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.src">src</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.dst">dst</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.id">id</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.properties">properties</strong></td>
<td valign="top"><a href="#gqlproperties">GqlProperties</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layernames">layerNames</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.layername">layerName</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.explode">explode</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.explodelayers">explodeLayers</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.history">history</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.deletions">deletions</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isvalid">isValid</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isactive">isActive</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isdeleted">isDeleted</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.isselfloop">isSelfLoop</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edge.nbr">nbr</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
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
</tbody>
</table>

### GqlConstantProperties

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
<td colspan="2" valign="top"><strong id="gqlconstantproperties.get">get</strong></td>
<td valign="top"><a href="#gqlprop">GqlProp</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlconstantproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlconstantproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlconstantproperties.values">values</strong></td>
<td valign="top">[<a href="#gqlprop">GqlProp</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### GqlEdges

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
<td colspan="2" valign="top"><strong id="gqledges.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.layers">layers</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.layer">layer</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.window">window</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqledges.at">at</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.latest">latest</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.before">before</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.after">after</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqledges.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.applyviews">applyViews</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#edgesviewcollection">EdgesViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.explode">explode</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.explodelayers">explodeLayers</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.sorted">sorted</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#edgesortby">EdgeSortBy</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqledges.page">page</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
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
<td colspan="2" valign="top"><strong id="gqledges.list">list</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlGraph

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
<td colspan="2" valign="top"><strong id="gqlgraph.uniquelayers">uniqueLayers</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.layers">layers</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.layer">layer</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.subgraph">subgraph</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.subgraphid">subgraphId</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.subgraphnodetypes">subgraphNodeTypes</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.excludenodes">excludeNodes</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.excludenodesid">excludeNodesId</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodes</td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.window">window</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td>

Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch

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
<td colspan="2" valign="top"><strong id="gqlgraph.at">at</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.latest">latest</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.before">before</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.after">after</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.created">created</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.lastopened">lastOpened</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.lastupdated">lastUpdated</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.earliesttime">earliestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.latesttime">latestTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.earliestedgetime">earliestEdgeTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.latestedgetime">latestEdgeTime</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">includeNegative</td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.countedges">countEdges</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.counttemporaledges">countTemporalEdges</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.countnodes">countNodes</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.hasnode">hasNode</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.hasnodeid">hasNodeId</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">id</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.hasedge">hasEdge</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.hasedgeid">hasEdgeId</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.node">node</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.nodeid">nodeId</strong></td>
<td valign="top"><a href="#node">Node</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">id</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.nodes">nodes</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td>

query (optionally a subset of) the nodes in the graph

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">ids</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.edgeid">edgeId</strong></td>
<td valign="top"><a href="#edge">Edge</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">src</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">dst</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.edges">edges</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.properties">properties</strong></td>
<td valign="top"><a href="#gqlproperties">GqlProperties</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.path">path</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.namespace">namespace</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.schema">schema</strong></td>
<td valign="top"><a href="#graphschema">GraphSchema</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.algorithms">algorithms</strong></td>
<td valign="top"><a href="#graphalgorithmplugin">GraphAlgorithmPlugin</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.sharedneighbours">sharedNeighbours</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">selectedNodes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.exportto">exportTo</strong></td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">property</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">condition</td>
<td valign="top"><a href="#filtercondition">FilterCondition</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">property</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">condition</td>
<td valign="top"><a href="#filtercondition">FilterCondition</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraph.searchnodes">searchNodes</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.searchedges">searchEdges</strong></td>
<td valign="top">[<a href="#edge">Edge</a>!]!</td>
<td></td>
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
<td colspan="2" valign="top"><strong id="gqlgraph.applyviews">applyViews</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#graphviewcollection">GraphViewCollection</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlGraphMetadata

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
<td colspan="2" valign="top"><strong id="gqlgraphmetadata.nodecount">nodeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphmetadata.edgecount">edgeCount</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphmetadata.properties">properties</strong></td>
<td valign="top">[<a href="#gqlprop">GqlProp</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlGraphs

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
<td colspan="2" valign="top"><strong id="gqlgraphs.name">name</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphs.path">path</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphs.namespace">namespace</strong></td>
<td valign="top">[<a href="#string">String</a>]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphs.created">created</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphs.lastopened">lastOpened</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlgraphs.lastupdated">lastUpdated</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlMutableEdge

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
<td colspan="2" valign="top"><strong id="gqlmutableedge.success">success</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Use to check if adding the edge was successful

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.edge">edge</strong></td>
<td valign="top"><a href="#edge">Edge</a>!</td>
<td>

Get the non-mutable edge for querying

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.src">src</strong></td>
<td valign="top"><a href="#gqlmutablenode">GqlMutableNode</a>!</td>
<td>

Get the mutable source node of the edge

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.dst">dst</strong></td>
<td valign="top"><a href="#gqlmutablenode">GqlMutableNode</a>!</td>
<td>

Get the mutable destination node of the edge

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.delete">delete</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Mark the edge as deleted at time `time`

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
<td colspan="2" valign="top"><strong id="gqlmutableedge.addconstantproperties">addConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add constant properties to the edge (errors if the value already exists)

If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.updateconstantproperties">updateConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update constant properties of the edge (existing values are overwritten)

If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
need to be specified again.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutableedge.addupdates">addUpdates</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add temporal property updates to the edge

If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
</tbody>
</table>

### GqlMutableGraph

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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.graph">graph</strong></td>
<td valign="top"><a href="#gqlgraph">GqlGraph</a>!</td>
<td>

Get the non-mutable graph

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.node">node</strong></td>
<td valign="top"><a href="#gqlmutablenode">GqlMutableNode</a></td>
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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addnode">addNode</strong></td>
<td valign="top"><a href="#gqlmutablenode">GqlMutableNode</a>!</td>
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.createnode">createNode</strong></td>
<td valign="top"><a href="#gqlmutablenode">GqlMutableNode</a>!</td>
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeType</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addnodes">addNodes</strong></td>
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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.edge">edge</strong></td>
<td valign="top"><a href="#gqlmutableedge">GqlMutableEdge</a></td>
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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addedge">addEdge</strong></td>
<td valign="top"><a href="#gqlmutableedge">GqlMutableEdge</a>!</td>
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">layer</td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addedges">addEdges</strong></td>
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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.deleteedge">deleteEdge</strong></td>
<td valign="top"><a href="#gqlmutableedge">GqlMutableEdge</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addproperties">addProperties</strong></td>
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.addconstantproperties">addConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add constant properties to graph (errors if the property already exists)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablegraph.updateconstantproperties">updateConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update constant properties of the graph (overwrites existing values)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlMutableNode

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
<td colspan="2" valign="top"><strong id="gqlmutablenode.success">success</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Use to check if adding the node was successful

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablenode.node">node</strong></td>
<td valign="top"><a href="#node">Node</a>!</td>
<td>

Get the non-mutable `Node`

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablenode.addconstantproperties">addConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Add constant properties to the node (errors if the property already exists)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablenode.setnodetype">setNodeType</strong></td>
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
<td colspan="2" valign="top"><strong id="gqlmutablenode.updateconstantproperties">updateConstantProperties</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td>

Update constant properties of the node (overwrites existing property values)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">properties</td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlmutablenode.addupdates">addUpdates</strong></td>
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
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### GqlNodes

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
<td colspan="2" valign="top"><strong id="gqlnodes.defaultlayer">defaultLayer</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.layers">layers</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.layer">layer</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.window">window</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlnodes.at">at</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.latest">latest</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.before">before</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.after">after</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlnodes.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">property</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">condition</td>
<td valign="top"><a href="#filtercondition">FilterCondition</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.applyviews">applyViews</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">views</td>
<td valign="top">[<a href="#nodesviewcollection">NodesViewCollection</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.sorted">sorted</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">sortBys</td>
<td valign="top">[<a href="#nodesortby">NodeSortBy</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
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
<td colspan="2" valign="top"><strong id="gqlnodes.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlnodes.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlPathFromNode

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
<td colspan="2" valign="top"><strong id="gqlpathfromnode.layers">layers</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.excludelayers">excludeLayers</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">names</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.layer">layer</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.excludelayer">excludeLayer</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">name</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.window">window</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlpathfromnode.at">at</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.latest">latest</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.before">before</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.after">after</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">time</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.shrinkwindow">shrinkWindow</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
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
<td colspan="2" valign="top"><strong id="gqlpathfromnode.shrinkstart">shrinkStart</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.shrinkend">shrinkEnd</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.typefilter">typeFilter</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">nodeTypes</td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.start">start</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.end">end</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.count">count</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.page">page</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
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
<td colspan="2" valign="top"><strong id="gqlpathfromnode.list">list</strong></td>
<td valign="top">[<a href="#node">Node</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpathfromnode.ids">ids</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlProp

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
<td colspan="2" valign="top"><strong id="gqlprop.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlprop.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlprop.value">value</strong></td>
<td valign="top"><a href="#gqlpropoutputval">GqlPropOutputVal</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlPropTuple

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
<td colspan="2" valign="top"><strong id="gqlproptuple.time">time</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproptuple.asstring">asString</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproptuple.value">value</strong></td>
<td valign="top"><a href="#gqlpropoutputval">GqlPropOutputVal</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlProperties

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
<td colspan="2" valign="top"><strong id="gqlproperties.get">get</strong></td>
<td valign="top"><a href="#gqlprop">GqlProp</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproperties.values">values</strong></td>
<td valign="top">[<a href="#gqlprop">GqlProp</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproperties.temporal">temporal</strong></td>
<td valign="top"><a href="#gqltemporalproperties">GqlTemporalProperties</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlproperties.constant">constant</strong></td>
<td valign="top"><a href="#gqlconstantproperties">GqlConstantProperties</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlTemporalProp

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
<td colspan="2" valign="top"><strong id="gqltemporalprop.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.history">history</strong></td>
<td valign="top">[<a href="#int">Int</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.values">values</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.at">at</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">t</td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.latest">latest</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.unique">unique</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalprop.ordereddedupe">orderedDedupe</strong></td>
<td valign="top">[<a href="#gqlproptuple">GqlPropTuple</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">latestTime</td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlTemporalProperties

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
<td colspan="2" valign="top"><strong id="gqltemporalproperties.get">get</strong></td>
<td valign="top"><a href="#gqltemporalprop">GqlTemporalProp</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalproperties.contains">contains</strong></td>
<td valign="top"><a href="#boolean">Boolean</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">key</td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalproperties.keys">keys</strong></td>
<td valign="top">[<a href="#string">String</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqltemporalproperties.values">values</strong></td>
<td valign="top">[<a href="#gqltemporalprop">GqlTemporalProp</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">keys</td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
</tbody>
</table>

### GqlVectorisedGraph

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
<td colspan="2" valign="top"><strong id="gqlvectorisedgraph.algorithms">algorithms</strong></td>
<td valign="top"><a href="#vectoralgorithmplugin">VectorAlgorithmPlugin</a>!</td>
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
<td colspan="2" valign="top"><strong id="graph.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
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
<td colspan="2" valign="top"><strong id="metagraph.metadata">metadata</strong></td>
<td valign="top"><a href="#gqlgraphmetadata">GqlGraphMetadata</a>!</td>
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
<td valign="top">[<a href="#metagraph">MetaGraph</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.path">path</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.parent">parent</strong></td>
<td valign="top"><a href="#namespace">Namespace</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="namespace.children">children</strong></td>
<td valign="top">[<a href="#namespace">Namespace</a>!]!</td>
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
<td valign="top"><a href="#gqlproperties">GqlProperties</a>!</td>
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
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outcomponent">outComponent</strong></td>
<td valign="top"><a href="#gqlnodes">GqlNodes</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.edges">edges</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outedges">outEdges</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inedges">inEdges</strong></td>
<td valign="top"><a href="#gqledges">GqlEdges</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.neighbours">neighbours</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.inneighbours">inNeighbours</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="node.outneighbours">outNeighbours</strong></td>
<td valign="top"><a href="#gqlpathfromnode">GqlPathFromNode</a>!</td>
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
<td colspan="2" valign="top"><strong id="queryplugin.globalsearch">globalSearch</strong></td>
<td valign="top">[<a href="#document">Document</a>!]!</td>
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
<td colspan="2" valign="top"><strong id="queryplugin.customsearch">customSearch</strong></td>
<td valign="top">[<a href="#document">Document</a>!]!</td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">query</td>
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

### VectorAlgorithmPlugin

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
<td colspan="2" valign="top"><strong id="vectoralgorithmplugin.similaritysearch">similaritySearch</strong></td>
<td valign="top">[<a href="#document">Document</a>!]!</td>
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
<td colspan="2" align="right" valign="top">start</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">end</td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
</tbody>
</table>

## Inputs

### ConstantPropertyFilterExpr

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
<td colspan="2" valign="top"><strong id="constantpropertyfilterexpr.name">name</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="constantpropertyfilterexpr.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="constantpropertyfilterexpr.value">value</strong></td>
<td valign="top"><a href="#value">Value</a></td>
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
<td colspan="2" valign="top"><strong id="edgeaddition.constantproperties">constantProperties</strong></td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#tpropinput">TpropInput</a>!]</td>
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
<td colspan="2" valign="top"><strong id="edgefilter.constantproperty">constantProperty</strong></td>
<td valign="top"><a href="#constantpropertyfilterexpr">ConstantPropertyFilterExpr</a></td>
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
<td colspan="2" valign="top"><strong id="edgeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
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
<td colspan="2" valign="top"><strong id="edgesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="edgesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
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

### FilterCondition

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
<td colspan="2" valign="top"><strong id="filtercondition.operator">operator</strong></td>
<td valign="top"><a href="#operator">Operator</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="filtercondition.value">value</strong></td>
<td valign="top"><a href="#value">Value</a></td>
<td></td>
</tr>
</tbody>
</table>

### FilterProperty

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
<td colspan="2" valign="top"><strong id="filterproperty.property">property</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="filterproperty.condition">condition</strong></td>
<td valign="top"><a href="#filtercondition">FilterCondition</a>!</td>
<td></td>
</tr>
</tbody>
</table>

### GqlPropInput

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
<td colspan="2" valign="top"><strong id="gqlpropinput.key">key</strong></td>
<td valign="top"><a href="#string">String</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="gqlpropinput.value">value</strong></td>
<td valign="top"><a href="#value">Value</a>!</td>
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
<td colspan="2" valign="top"><strong id="graphviewcollection.subgraphid">subgraphId</strong></td>
<td valign="top">[<a href="#int">Int</a>!]</td>
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
<td colspan="2" valign="top"><strong id="graphviewcollection.excludenodesid">excludeNodesId</strong></td>
<td valign="top">[<a href="#int">Int</a>!]</td>
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
<td valign="top"><a href="#filterproperty">FilterProperty</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="graphviewcollection.edgefilter">edgeFilter</strong></td>
<td valign="top"><a href="#filterproperty">FilterProperty</a></td>
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
<td colspan="2" valign="top"><strong id="nodeaddition.constantproperties">constantProperties</strong></td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeaddition.updates">updates</strong></td>
<td valign="top">[<a href="#tpropinput">TpropInput</a>!]</td>
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
<td valign="top"><a href="#string">String</a>!</td>
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
<td colspan="2" valign="top"><strong id="nodefilter.constantproperty">constantProperty</strong></td>
<td valign="top"><a href="#constantpropertyfilterexpr">ConstantPropertyFilterExpr</a></td>
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
<td colspan="2" valign="top"><strong id="nodeviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodeviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
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
<td colspan="2" valign="top"><strong id="nodesviewcollection.latest">latest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotat">snapshotAt</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.snapshotlatest">snapshotLatest</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
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
<td colspan="2" valign="top"><strong id="nodesviewcollection.typefilter">typeFilter</strong></td>
<td valign="top">[<a href="#string">String</a>!]</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="nodesviewcollection.nodefilter">nodeFilter</strong></td>
<td valign="top"><a href="#filterproperty">FilterProperty</a></td>
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

### TpropInput

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
<td colspan="2" valign="top"><strong id="tpropinput.time">time</strong></td>
<td valign="top"><a href="#int">Int</a>!</td>
<td></td>
</tr>
<tr>
<td colspan="2" valign="top"><strong id="tpropinput.properties">properties</strong></td>
<td valign="top">[<a href="#gqlpropinput">GqlPropInput</a>!]</td>
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

## Enums

### GqlGraphType

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
<td valign="top"><strong>ANY</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>NOT_ANY</strong></td>
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

### GqlPropOutputVal

### Int

The `Int` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1.

### String

The `String` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.

### Upload


## Unions

### GqlDocumentEntity

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
<td></td>
</tr>
<tr>
<td valign="top"><strong><a href="#graph">Graph</a></strong></td>
<td></td>
</tr>
</tbody>
</table>


<!-- END graphql-markdown -->
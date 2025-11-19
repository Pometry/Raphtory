# User Interface overview

## Search page

![UI Search page](../../assets/images/raphtory_ui_search_baboon_attacks.png)

The **Search page** consists of the following elements:

- **Global menu** - Switch between any of the main pages.
- **Search results** - Shows the results of your query.
- **Pinned results** - Shows any results you have pinned.
- **Query Builder** - Search  a selected graph using the options provided.
- **Selected** - Shows contextual information about the current selection.

## Graphs page

![UI Search page](../../assets/images/raphtory_ui_graph_list.png)

The **Graphs page** consists of the following elements:

- **Global menu** - Switch between any of the main pages.
- **Graphs list** - Shows the available graphs.
- **Context menu** - Shows contextual information about the current selection.

## Graph view

![UI Search page](../../assets/images/raphtory_ui_graph_view.png)

The **Graph view** displays the graph or sub-graph you have selected and provides information on that selection. You can also refine your selection further or save it as a new graph.

The **Graph view** consists of the following elements:

- **Global menu** - Switch between any of the main pages.
- **Toolbar** - Manipulate the current selection.
- **Context menu** - Shows contextual information about the current selection.
    - **Overview** - Information the currently selected graph or sub-graph.
    - **Layout** - Modify how the layout engine displays the graph.
    - **Selected** - Information about the selected node or edge.
- **Graph canvas** - Displays the current graph or sub-graph. You can select a node or edge to show it's information in the **Context menu**.
- **Temporal view** - Displays the edges of the current graph or sub-graph as a timeline of events. On longer timescales edges are shown as a heatmap instead of discrete events.

### Modifying the Layout

The Raphtory UI gives you detailed control over how your graphs are displayed. You can use this to match your preferences, build custom visualisations for reports or better fit the shape of your data.

Raphtory's layout engine is built on [G6](https://github.com/antvis/G6) and many of the [D3 Force-Directed Layout](https://g6.antv.antgroup.com/en/manual/layout/d3-force-layout) parameters are exposed in the **Layout** tab of the **Context menu**.

You can select from the following layout algorithms:

- Default
- Concentric
- Force Based
- Hierarchical TD
- Hierarchical LR

For each layout the following parameters can be set:

| Parameter             | Description                                                                                                                                                     |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Collision Radius      | The collision force treats nodes as circles with a given radius, rather than points, and prevents nodes from overlapping. You can specify the effective radius. |
| Collision Strength    | Sets the strength of the collision force.                                                                                                                       |
| Link Distance         |                                                                                                                                                                 |
| Link Strength         |                                                                                                                                                                 |
| Many-Body Force       |                                                                                                                                                                 |
| Many-Body Range       |                                                                                                                                                                 |
| Center Force          |                                                                                                                                                                 |
| Radial Force Strength |                                                                                                                                                                 |
| Radial Force Radius   |                                                                                                                                                                 |

Optionally, you can set a pre-layout algorithm:

- Concentric
- Dagre LR
- Dagre TB

For each pre-layout algorithm the following parameters can be set:

| Parameter                  | Description |
|----------------------------|-------------|
| Use Clockwise              |             |
| Maintain Equidistant Rings |             |
| Node Size                  |             |
| Node Spacing               |             |
| Prevent Overlap            |             |
| Start Angle                |             |
| Sweep                      |             |


## GraphQL playground

![UI Search page](../../assets/images/raphtory_ui_graphiql_playground_query.png)

This page allows you to access the standard [GraphiQL](https://github.com/graphql/graphiql) playground.

# Graph view

![UI Search page](../../assets/images/raphtory_ui_graph_view_v0_16_4.png)

The **Graph view** consists of the following elements:

- **Global menu** - Switch between any of the main pages.
- **Toolbar** - Manipulate the current selection.
- **Context menu** - Shows contextual information about the current selection.
    - **Overview** - Information the currently selected graph or sub-graph.
    - **Layout** - Modify how the layout engine displays the graph.
    - **Selected** - Information about the selected node or edge.
    - **Graph settings** - Modify the style properties of a selected node or edge.
- **Graph canvas** - Displays the current graph or sub-graph. You can select a node or edge to show it's information in the **Context menu**.
- **Temporal view** - Displays the edges of the current graph or sub-graph as a timeline of events. On longer timescales edges are shown as a heatmap instead of discrete events.

## Modifying the graph Layout

![Layout tab of the context menu](../../assets/images/raphtory_ui_graph_view_layout.png)

The Raphtory UI gives you detailed control over how your graphs are displayed. You can use this to match your preferences, build custom visualisations for reports or better fit the shape of your data.

Raphtory's layout engine is built on [G6](https://github.com/antvis/G6) and many of the [D3 Force-Directed Layout](https://g6.antv.antgroup.com/en/manual/layout/d3-force-layout) parameters are exposed in the **Layout** tab of the **Context menu**.

You can select from the following layout algorithms:

- Default
- Concentric
- Force Based
- Hierarchical TD
- Hierarchical LR

For each layout, specific **Advanced Options** can be set to tune the algorithm.

### Default Layout

| Parameter             | Description                                                                                                                                                     |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Collision Radius      | The collision force treats nodes as circles with a given radius, rather than points, and prevents nodes from overlapping. You can specify the effective radius. |
| Collision Strength    | Sets the strength of the collision force.                                                                                                                       |
| Link Distance         | Specify an ideal edge length.                                                                                                                                   |
| Link Strength         | Higher link strength results in distances closer to the ideal.                                                                                                  |
| Many-Body Force       | The mutual force between nodes, a positive value is attractive and a negative value is repulsive.                                                               |
| Many-Body Range       | Set a maximum and minimum distance between nodes that where many-body forces are applied.                                                                       |
| Center Force          | Applies a uniform force on all nodes towards the center.                                                                                                        |
| Radial Force Strength | Applies a uniform force on all nodes within a specified radius towards the center.                                                                              |
| Radial Force Radius   | Specify a radius for the radial force.                                                                                                                          |

### Concentric Layout

| Parameter                  | Description                                                              |
|----------------------------|--------------------------------------------------------------------------|
| Use Clockwise              | Enable to add nodes in a clockwise order.                                |
| Maintain Equidistant Rings | Enable to require equidistant rings.                                     |
| Node Size                  | Effective node diameter. This effects ring spacing to avoid collision.   |
| Node Spacing               | Minimum spacing between rings.                                           |
| Prevent Overlap            | Enable to prevent overlap between nodes. Only works if Node Size is set. |
| Start Angle                | Start angle where the first node is added. Specified in radians.         |
| Sweep                      | Angle between the first and last nodes in the same ring.                 |

### Force Based Layout

| Parameter | Description                                                                                     |
|-----------|-------------------------------------------------------------------------------------------------|
| Gravity   | Applies a force on all nodes towards the center proportional to their distance from the center. |
| Speed     | Movement speed per iteration of the algorithm.                                                  |

### Hierarchical TB Layout

| Parameter                  | Description                                          |
|----------------------------|------------------------------------------------------|
| Invert                     | Enable to invert the direction.                      |
| Direction                  | Specify how the node hierarchy should be aligned.    |
| Node Separation            | Separation of nodes in the same rank.                |
| Rank Separation            | Separation between ranks.                            |
| Rank algorithm             | Specify the algorithm used to assign nodes to ranks. |
| Node Size                  | Node size used for collision.                        |
| Retain Edge Control Points | Enable to use control points.                        |

### Pre-layout algorithms

Optionally, you can set a pre-layout algorithm that runs before the primary layout algorithm:

- Concentric - arranged around the center.
- Dagre LR - arranged using the hierarchical Dagre algorithm from left to right.
- Dagre TB - arranged using the hierarchical Dagre algorithm from top to bottom.

For Concentric and Dagre TB algorithms you can also specify **Advanced Options** when used in the pre-layout phase.

## Modify styles

![Graph settings tab of the Context menu](../../assets/images/raphtory_ui_graph_view_settings.png)

You can modify the styles applied to nodes and edges from the **Graph settings** tab of the **Context menu**.

You can perform both global and local changes which are saved as metadata in the graph. Style metadata for node types is stored on the graph, for edge layers is stored on each edge, and for individual nodes is stored on the matching node.

The format for styles is as follows:

```python
# Graph styles
{'nodeStylePerson': {'nodeType': 'Person', 'fill': '#1cb917', 'size': 24}, 'nodeStyleNone': {}}
# Node styles
{'style': {'fill': '#417505', 'size': 12}}
# Edge styles
{'style': [None, {'meets': {'startArrowSize': 4, 'stroke': '#f8e61b', 'lineWidth': 1, 'endArrowSize': 4}}, None, None]}
```

### Set the styles for a specified node type

1. Clear all selections.
2. Switch to the **Graph settings** tab of the **Context menu**
3. Click the **Select Node Type** drop down and choose a node type or 'None'.
4. Specify a colour using the **Node Colour** palette.
5. Specify a **Node Size** value.
6. Click **Save**.

### Set the styles for a specified edge layer

1. Select any edge.
2. Switch to the **Graph settings** tab of the **Context menu**
3. Click the **Select Edge Layer** dropdown and choose a layer.
4. Specify a colour using the **Node Colour** palette.
5. Specify a **Node Size** value.
6. Click **Save**.

### Set the styles for the currently selected node

1. Select any node.
2. Switch to the **Graph settings** tab of the **Context menu**
3. Specify a colour using the **Node Colour** palette.
4. Specify a **Node Size** value.
5. Click **Save**.

!!! Note
    Styles set on an individual node override styles set on a node type. Additionally, styles can only be applied to individual nodes, if you have multiple nodes selected the last node you selected will be updated.

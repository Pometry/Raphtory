"""
Generate a visualisation using matplotlib or pyvis from Raphtory graphs.
"""
from pyvis.network import Network
import networkx as nx
import pandas as pd


def to_pyvis(
    graph,
    explode_edges=False,
    edge_color="#000000",
    shape=None,
    node_image=None,
    edge_weight=None,
    edge_label=None,
    colour_nodes_by_type=False,
    type_property="type",
    notebook=True,
    **kwargs,
):
    r"""Draw a graph with Pyvis.

    .. note::

        Pyvis is a required dependency.
        If you intend to use this function make sure that
        you install Pyvis with ``pip install pyvis``

    :param graph: A Raphtory graph.
    :param explode_edges: A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.
    :param str edge_color: A string defining the colour of the edges in the graph. By default ``#000000`` (black) is set.
    :param str shape: An optional string defining what the node looks like.
    There are two types of nodes. One type has the label inside of it and the other type has the label underneath it.
    The types with the label inside of it are: ellipse, circle, database, box, text.
    The ones with the label outside of it are: image, circularImage, diamond, dot, star, triangle, triangleDown, square and icon.
    By default ``"dot"`` is set.
    :param str node_image: An optional string defining the url of a custom node image. By default an image of a circle is set.
    :param str edge_weight: An optional string defining the name of the property where edge weight is set on your Raphtory graph. By default ``1`` is set.
    :param str edge_label: An optional string defining the name of the property where edge label is set on your Raphtory graph. By default, an empty string as the label is set.
    :param bool notebook: A boolean that is set to True if using jupyter notebook. By default this is set to True.
    :param kwargs: Additional keyword arguments that are passed to the pyvis Network class.

    :returns: A pyvis network

    For Example:

    .. code-block:: python

        from raphtory import Graph
        from raphtory import export

        g = Graph()
        g.add_node(1, src, properties={"image": "image.png"})
        g.add_edge(1, 1, 2, {"title": "edge", "weight": 1})
        g.add_edge(1, 2, 1, {"title": "edge", "weight": 3})

        export.to_pyvis(graph=g, edge_color="#FF0000", edge_weight= "weight", shape="image", node_image="image", edge_label="title")

    """
    visGraph = Network(notebook=notebook, **kwargs)
    if colour_nodes_by_type:
        groups = {
            value: index + 1
            for index, value in enumerate(
                set(graph.nodes.properties.get(type_property))
            )
        }

    for v in graph.nodes:
        image = (
            v.properties.get(node_image)
            if node_image != None
            else "https://cdn-icons-png.flaticon.com/512/7584/7584620.png"
        )
        shape = shape if shape is not None else "dot"
        if colour_nodes_by_type:
            visGraph.add_node(
                v.id,
                label=v.name,
                shape=shape,
                image=image,
                group=groups[v.properties.get(type_property)],
            )
        else:
            visGraph.add_node(v.id, label=v.name, shape=shape, image=image)

    edges = graph.edges.explode() if explode_edges else graph.edges.explode_layers()
    for e in edges:
        weight = e.properties.get(edge_weight) if edge_weight is not None else 1
        if weight is None:
            weight = 1
        label = e.properties.get(edge_label) if edge_label is not None else ""
        if label is None:
            label = ""
        visGraph.add_edge(
            e.src.id,
            e.dst.id,
            value=weight,
            color=edge_color,
            title=label,
            arrowStrikethrough=False,
        )

    return visGraph

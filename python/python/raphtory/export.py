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

    .. jupyter-execute::

    from raphtory import Graph
    from raphtory import export

    g = Graph()
    g.add_vertex(1, src, properties={"image": "image.png"})
    g.add_edge(1, 1, 2, {"title": "edge", "weight": 1})
    g.add_edge(1, 2, 1, {"title": "edge", "weight": 3})

    export.to_pyvis(graph=g, edge_color="#FF0000", edge_weight= "weight", shape="image", node_image="image", edge_label="title")

    """
    visGraph = Network(notebook=notebook, **kwargs)
    if colour_nodes_by_type:
        groups = {
            value: index + 1
            for index, value in enumerate(
                set(graph.vertices.properties.get(type_property))
            )
        }

    for v in graph.vertices():
        image = (
            v.properties.get(node_image)
            if node_image != None
            else "https://cdn-icons-png.flaticon.com/512/7584/7584620.png"
        )
        shape = shape if shape is not None else "dot"
        if colour_nodes_by_type:
            visGraph.add_node(
                v.id(),
                label=v.name(),
                shape=shape,
                image=image,
                group=groups[v.properties.get(type_property)],
            )
        else:
            visGraph.add_node(v.id(), label=v.name(), shape=shape, image=image)

    edges = graph.edges().explode() if explode_edges else graph.edges().explode_layers()
    for e in edges:
        weight = e.properties.get(edge_weight) if edge_weight is not None else 1
        if weight is None:
            weight = 1
        label = e.properties.get(edge_label) if edge_label is not None else ""
        if label is None:
            label = ""
        visGraph.add_edge(
            e.src().id(),
            e.dst().id(),
            value=weight,
            color=edge_color,
            title=label,
            arrowStrikethrough=False,
        )

    return visGraph


def to_networkx(
    graph,
    explode_edges=False,
    include_vertex_properties=True,
    include_edge_properties=True,
    include_update_history=True,
    include_property_histories=True,
):
    r"""Returns a graph with NetworkX.
    .. note::

        Network X is a required dependency.
        If you intend to use this function make sure that
        you install Network X with ``pip install networkx``

    :param Graph graph: A Raphtory graph.
    :param bool explode_edges: A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.
    :param bool include_vertex_properties: A boolean that is set to True if you want to include the vertex properties in the graph. By default this is set to True.
    :param bool include_edge_properties: A boolean that is set to True if you want to include the edge properties in the graph. By default this is set to True.
    :param bool include_update_history: A boolean that is set to True if you want to include the update histories in the graph. By default this is set to True.
    :param bool include_property_histories: A boolean that is set to True if you want to include the histories in the graph. By default this is set to True.

    :returns: A Networkx MultiDiGraph.
    """

    networkXGraph = nx.MultiDiGraph()

    vertex_tuples = []
    for v in graph.vertices():
        properties = {}
        if include_vertex_properties:
            if include_property_histories:
                properties.update(v.properties.constant.as_dict())
                properties.update(v.properties.temporal.histories())
            else:
                properties = v.properties.as_dict()
        if include_update_history:
            properties.update({"update_history": v.history()})
        vertex_tuples.append((v.name(), properties))
    networkXGraph.add_nodes_from(vertex_tuples)

    edge_tuples = []
    edges = graph.edges().explode() if explode_edges else graph.edges().explode_layers()
    for e in edges:
        properties = {}
        src = e.src().name()
        dst = e.dst().name()
        if include_edge_properties:
            if include_property_histories:
                properties.update(e.properties.constant.as_dict())
                properties.update(e.properties.temporal.histories())
            else:
                properties = e.properties.as_dict()
        layer = e.layer_name()
        if layer is not None:
            properties.update({"layer": layer})
        if include_update_history:
            if explode_edges:
                properties.update({"update_history": e.time()})
            else:
                properties.update({"update_history": e.history()})
        edge_tuples.append((src, dst, properties))

    networkXGraph.add_edges_from(edge_tuples)

    return networkXGraph


def to_edge_df(
    graph,
    explode_edges=False,
    include_edge_properties=True,
    include_update_history=True,
    include_property_histories=True,
):
    r"""Returns an edge list pandas dataframe for the given graph.
    .. note::

        Pandas is a required dependency.
        If you intend to use this function make sure that
        you install pandas with ``pip install pandas``

    :param Graph graph: A Raphtory graph.
    :param bool explode_edges: A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.
    :param bool include_edge_properties: A boolean that is set to True if you want to include the edge properties in the graph. By default this is set to True.
    :param bool include_update_history: A boolean that is set to True if you want to include the update histories in the graph. By default this is set to True.
    :param bool include_property_histories: A boolean that is set to True if you want to include the histories in the graph. By default this is set to True.

    :returns: A pandas dataframe.
    """
    edge_tuples = []

    columns = ["src", "dst", "layer"]
    if include_edge_properties:
        columns.append("properties")
    if include_update_history:
        columns.append("update_history")

    edges = graph.edges().explode() if explode_edges else graph.edges().explode_layers()
    for e in edges:
        tuple = [e.src().name(), e.dst().name(), e.layer_name()]
        if include_edge_properties:
            properties = {}
            if include_property_histories:
                properties.update(e.properties.constant.as_dict())
                properties.update(e.properties.temporal.histories())
            else:
                properties = e.properties.as_dict()
            tuple.append(properties)

        if include_update_history:
            if explode_edges:
                tuple.append(e.time())
            else:
                tuple.append(e.history())

        edge_tuples.append(tuple)

    return pd.DataFrame(edge_tuples, columns=columns)


def to_vertex_df(
    graph,
    include_vertex_properties=True,
    include_update_history=True,
    include_property_histories=True,
):
    r"""Returns an vertex list pandas dataframe for the given graph.

    .. note::

        Pandas is a required dependency.
        If you intend to use this function make sure that
        you install pandas with ``pip install pandas``

    :param Graph graph: A Raphtory graph.
    :param bool include_vertex_properties: A boolean that is set to True if you want to include the vertex properties in the graph. By default this is set to True.
    :param bool include_update_history: A boolean that is set to True if you want to include the update histories in the graph. By default this is set to True.
    :param bool include_property_histories: A boolean that is set to True if you want to include the histories in the graph. By default this is set to True.

    :returns: A pandas dataframe.

    """
    vertex_tuples = []
    columns = ["id"]
    if include_vertex_properties:
        columns.append("properties")
    if include_update_history:
        columns.append("update_history")

    for v in graph.vertices():
        tuple = [v.name()]
        if include_vertex_properties:
            properties = {}
            if include_property_histories:
                properties.update(v.properties.constant.as_dict())
                properties.update(v.properties.temporal.histories())
            else:
                properties = v.properties.as_dict()
            tuple.append(properties)
        if include_update_history:
            tuple.append(v.history())
        vertex_tuples.append(tuple)
    return pd.DataFrame(vertex_tuples, columns=columns)

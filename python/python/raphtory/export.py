"""
Generate a visualisation using matplotlib or pyvis from Raphtory graphs.
"""
from pyvis.network import Network
import networkx as nx
import pandas as pd

r"""Draw a graph with Pyvis.

.. note::

    Pyvis is a required dependency.
    If you intend to use this function make sure that
    you install Pyvis with ``pip install pyvis``

:param graph: A Raphtory graph.
:param str height: A string defining the height of the graph. By default ``800px`` is set.
:param str width: A string defining the width of the graph.  By default ``800px`` is set.
:param str bg_color: A string defining the colour of the graph background. It must be a HTML color code. By default ``#white`` (white) is set.
:param str font_color: A string defining the colour of the graph font. By default ``"black"`` is set.
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


:returns: A pyvis visualisation in static HTML format that is interactive with toggles menu.
:rtype: IFrame(name, width=self.width, height=self.height)

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
        **kwargs
):
    """
    Returns a dynamic visualisation in static HTML format from a Raphtory graph.
    """
    visGraph = Network(notebook=notebook, **kwargs)
    if colour_nodes_by_type:
        groups = {value: index + 1 for index, value in enumerate(set(graph.vertices.properties.get(type_property)))}

    for v in graph.vertices():
        image = v.properties.get(node_image) if node_image != None else "https://cdn-icons-png.flaticon.com/512/7584/7584620.png"
        shape = shape if shape is not None else "dot"
        if colour_nodes_by_type:
            visGraph.add_node(v.id(), label= v.name(), shape=shape, image=image, group=groups[v.properties.get(type_property)])
        else:
            visGraph.add_node(v.id(), label= v.name(), shape=shape, image=image)

    edges = graph.edges().explode() if explode_edges else graph.edges().explode_layers()
    for e in edges:
        weight = e.properties.get(edge_weight) if edge_weight is not None else 1
        if weight is None:
            weight = 1
        label = e.properties.get(edge_label) if edge_label is not None else ""
        if label is None:
            label = ""
        visGraph.add_edge(e.src().id(), e.dst().id(), value=weight, color=edge_color, title=label, arrowStrikethrough=False)

    return visGraph

r"""Returns a graph with NetworkX.

.. note::

    Network X is a required dependency.
    If you intend to use this function make sure that
    you install Network X with ``pip install networkx``

:param Graph graph: A Raphtory graph.
:param bool explode_edges: A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.

:returns: A Networkx MultiDiGraph.

"""
def to_networkx(
        graph,
        explode_edges=False,
        include_vertex_properties=True,
        include_edge_properties=True,
        include_histories=True,
):
    """
    Returns a Network X graph from a Raphtory graph.
    """

    networkXGraph = nx.MultiDiGraph()

    vertex_tuples = []
    for v in graph.vertices():
        properties = {}
        if include_vertex_properties:
            if include_histories:
                properties = merge(v.properties.constant.as_dict(),v.properties.temporal.histories())
            else:
                properties = v.properties.as_dict()
        vertex_tuples.append((v.name(), properties))
    networkXGraph.add_nodes_from(vertex_tuples)

    edge_tuples = []
    edges = graph.edges().explode() if explode_edges else graph.edges().explode_layers()
    for e in edges:
        properties = {}
        src = e.src().name()
        dst = e.dst().name()
        layer = e.layer_name()
        
        if include_edge_properties:
            if include_histories:
                properties = merge(v.properties.constant.as_dict(),v.properties.temporal.histories())
            else:
                properties = v.properties.as_dict()
        edge_tuples.append((src, dst, properties))

    networkXGraph.add_edges_from(edge_tuples)

    return networkXGraph

def to_edge_list_df(
        graph,
):
    """
    Returns a list of edges from a Raphtory graph in Pandas dataframe format.
    """
    edge_list = []
   

    for e in graph.edges():
        e_constant_properties = []
        e_temporal_properties = []
        if e.layer_names():
            for layer in e.layer_names():
                src = e.src().name() if e.src() else None
                dst = e.dst().name() if e.dst() else None
                history = e.history() if e.history() else None

                if e.properties.constant is not None:
                    for key, value in e.properties.constant.items():
                        e_constant_properties.append((key, value))
                else:
                    e_constant_properties = None

                if e.properties.temporal is not None:
                    for prop, hist in e.properties.temporal.items():
                        for timestamp, value in hist:
                            e_temporal_properties.append((timestamp, prop, value))
                else:
                    e_temporal_properties = None

                edge_tuple = (src, dst, layer, history, e_constant_properties, e_temporal_properties)
                edge_list.append(edge_tuple)
    
    return pd.DataFrame(edge_list, columns=["src", "dst", "layer", "history", "constant_properties", "temporal_properties"])

def to_node_list_df(
        graph,
):
    """
    Returns a list of nodes from a Raphtory graph in Pandas dataframe format.
    """
    node_list = []
 

    for v in graph.vertices():
        v_constant_properties = []
        v_temporal_properties = []
        name = v.name() if v.name() else None
        history = v.history() if v.history() else None

        if v.properties.constant is not None:
            for key, value in v.properties.constant.items():
                v_constant_properties.append((key, value))
        else:
            v_constant_properties = None

        if v.properties.temporal is not None:
            for prop, hist in v.properties.temporal.items():
                for timestamp, value in hist:
                    v_temporal_properties.append((timestamp, prop, value))
        else:
            v_temporal_properties = None

        node_tuple = (name, history, v_constant_properties, v_temporal_properties)
        node_list.append(node_tuple)
    
    return pd.DataFrame(node_list, columns=["name", "history", "constant_properties", "temporal_properties"])
   
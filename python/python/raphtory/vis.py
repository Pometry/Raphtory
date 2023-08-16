"""
Generate a visualisation using matplotlib or pyvis from Raphtory graphs.
"""
from pyvis.network import Network
import networkx as nx

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
    from raphtory import vis

    g = Graph()
    g.add_vertex(1, src, properties={"image": "image.png"})
    g.add_edge(1, 1, 2, {"title": "edge", "weight": 1})
    g.add_edge(1, 2, 1, {"title": "edge", "weight": 3})

    vis.to_pyvis(graph=g, edge_color="#FF0000", edge_weight= "weight", shape="image", node_image="image", edge_label="title")

"""

def to_pyvis(
        graph,
        height="800px",
        width="800px",
        bg_color="#white",
        font_color="black",
        edge_color="#000000",
        shape=None,
        node_image=None,
        edge_weight=None,
        edge_label=None,
        notebook=True,
        colour_nodes_by_type=False,
        type_property="type",
):
    """
    Returns a dynamic visualisation in static HTML format from a Raphtory graph.
    """
    visGraph = Network(height=height, width=width, bgcolor=bg_color, font_color=font_color, notebook=notebook)
    if colour_nodes_by_type:
        groups = {value: index + 1 for index, value in enumerate(set(graph.vertices.properties.get(type_property)))}


    for v in graph.vertices():
        image = v.property(node_image) if node_image != None else "https://cdn-icons-png.flaticon.com/512/7584/7584620.png"
        shape = shape if shape != None else "dot"
        if colour_nodes_by_type:
            visGraph.add_node(v.id(), label= v.name(), shape=shape, image=image, group=groups[v.properties.get(type_property)])
        else:
            visGraph.add_node(v.id(), label= v.name(), shape=shape, image=image)

    for e in graph.edges():
        weight = e.property(edge_weight) if edge_weight != None else 1
        label = e.property(edge_label) if edge_label != None else ""
        visGraph.add_edge(e.src().id(), e.dst().id(), value=weight, color=edge_color, title=label)

    visGraph.show_buttons(filter_=['physics'])
    visGraph.show('nx.html')
    return visGraph

r"""Draw a graph with NetworkX.

.. note::

    Network X is a required dependency.
    If you intend to use this function make sure that
    you install Network X with ``pip install networkx``

:param graph: A Raphtory graph.
:param float k: A float defining optimal distance between nodes. If None the distance is set to 1/sqrt(n) where n is the number of nodes. Increase this value to move nodes farther apart.
:param int iterations: An integer defining the maximum number of iterations taken to generate the optimum spring layout. Increasing this number will increase the computational time to generate the layout.  By default ``50`` is set.
:param scalar or array node_size: A scalar defining the size of nodes. By default ``300`` is set.
:param color or array of colors node_color: Node color. Can be a single color or a sequence of colors with the same length as nodelist. Color can be string or rgb (or rgba) tuple of floats from 0-1. If numeric values are specified they will be mapped to colors using the cmap and vmin,vmax parameters. See matplotlib.scatter for more details. By default ``"#1f78b4"`` (blue) is set.
:param color or array of colors edge_color: Edge color. Can be a single color or a sequence of colors with the same length as edgelist. Color can be string or rgb (or rgba) tuple of floats from 0-1. If numeric values are specified they will be mapped to colors using the edge_cmap and edge_vmin,edge_vmax parameters. By default ``'k'`` (black) is set.
:param bool arrows: If None, directed graphs draw arrowheads with FancyArrowPatch, while undirected graphs draw edges via LineCollection for speed. If True, draw arrowheads with FancyArrowPatches (bendable and stylish). If False, draw edges using LineCollection (linear and fast).
    Note: Arrowheads will be the same color as edges. Default is None.
:param str arrow_style: Style of the edges, defaults to ``‘-|>’``.

:returns: A networkx visualisation that appears in the notebook output.
:rtype:  matplotlib.collections.PathCollection and matplotlib.collections.LineCollection or a list of matplotlib.patches.FancyArrowPatch.
        `PathCollection` of the nodes.  
        If ``arrows=True``, a list of FancyArrowPatches is returned.
        If ``arrows=False``, a LineCollection is returned.
        If ``arrows=None`` (the default), then a LineCollection is returned if
        `G` is undirected, otherwise returns a list of FancyArrowPatches.

For Example:

.. jupyter-execute::

    from raphtory import Graph
    from raphtory import vis

    g = Graph()
    g.add_vertex(1, src, properties={"image": "image.png"})
    g.add_edge(1, 1, 2, {"title": "edge", "weight": 1})
    g.add_edge(1, 2, 1, {"title": "edge", "weight": 3})

    vis.to_networkx(graph=g, k=0.15, iterations=100, node_size=500, node_color='red', edge_color='blue', arrows=True)

"""
def to_networkx(
        graph,
        k=None,
        iterations=50,
        node_size=300,
        node_color='#1f78b4',
        edge_color='k',
        arrows=None,
        arrow_style= "-|>"
):
    """
    Returns a Network X graph visualiation from a Raphtory graph.
    """

    networkXGraph = nx.MultiDiGraph()

    networkXGraph.add_nodes_from(list(graph.vertices().id()))

    edges = []
    for e in graph.edges():
        edges.append((e.src().id(), e.dst().id()))

    networkXGraph.add_edges_from(edges)
    pos = nx.spring_layout(networkXGraph, k=k, iterations=iterations)

    nx.draw_networkx_nodes(networkXGraph, pos, node_size=node_size, node_color=node_color)
    nx.draw_networkx_edges(networkXGraph, pos, edge_color=edge_color, arrows=arrows, arrowstyle=arrow_style)




from pyvis.network import Network

r"""Draw a graph with Pyvis.

.. note::

    Pyvis is an required dependency.
    If you intend to use this function make sure that
    you install pyvis with ``pip install pyvis``

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

    vis.draw(graph=g, edge_color="#FF0000", edge_weight= "weight", shape="image", node_image="image", edge_label="title")
    
"""

def draw(
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
        ):
    """
    Returns a dynamic visualisation in static HTML format from a Raphtory graph.
    """
    visGraph = Network(height=height, width=width, bgcolor=bg_color, font_color=font_color, notebook=notebook)
   
    for v in graph.vertices():
        image = v.property(node_image) if node_image != None else "https://cdn-icons-png.flaticon.com/512/7584/7584620.png"
        shape = shape if shape != None else "dot"
        visGraph.add_node(v.id(), label= v.name(), shape=shape, image=image)

    for e in graph.edges():
        weight = e.property(edge_weight) if edge_weight != None else 1
        label = e.property(edge_label) if edge_label != None else ""
        visGraph.add_edge(e.src().id(), e.dst().id(), value=weight, color=edge_color, title=label)
       
    visGraph.show_buttons(filter_=['physics'])
    visGraph.show('nx.html')
    

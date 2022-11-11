# Running PyRaphtory

This is a very short guide in how to setup a PyRaphtory graph. 

First we need to import PyRaphtory

    from pyraphtory.context import PyRaphtory

Next we create a graph

    ctx = PyRaphtory.local()
    graph = ctx.new_graph()

Now we can add nodes and edges

        graph.add_vertex(1, 1)
        graph.add_vertex(1, 2)
        graph.add_vertex(1, 3)
        graph.add_edge(2, 1, 2)
        graph.add_edge(2, 1, 3)

We can then now run a small query

    from pyraphtory.graph import Row
    df = graph \
    .select(lambda vertex: Row(vertex.name(), vertex.degree(), vertex.out_degree(), vertex.in_degree())) \
    .to_df(["name", "degree", "out_degree", "in_degree"])

The result of the dataframe

       timestamp window         name    degree  out_degree  in_degree
       0          2     None    1       2           2          0
       1          2     None    2       1           0          1
       2          2     None    3       1           0          1
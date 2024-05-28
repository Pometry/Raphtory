from raphtory import Graph, PersistentGraph
from raphtory.algorithms import dijkstra_single_source_shortest_paths
import tempfile

def main():
    # Create an instance of a Graph
    graph = Graph()

    # Add nodes to the graph with properties
    graph.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    graph.add_node(
        2,
        "ben",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    graph.add_node(
        3,
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )

    # Add edges between nodes with properties
    graph.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
    )
    graph.add_edge(
        1,
        "ben",
        "hamza",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    graph.add_edge(
        3,
        "ben",
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )
    
    # Add constant properties to a node 
    graph.node('ben').add_constant_properties({'static prop': 123})
    assert graph.node('ben').properties.get('static prop') == 123
    
    # Update/Add properties of a node
    props_t2 = {"value2": 100, "value_f": 15.0}
    graph.node("ben").add_updates(7, props_t2)
    assert graph.node("ben").properties.get('value2') == 100

    # Update properties of an edge
    props_t2 = {
        "value": 200,
        "value_str": "I wanna spock right now",
    }
    graph.edge('ben', 'haaroon').add_updates(7, props_t2)
    assert graph.edge('ben', 'haaroon').properties.get('value') == 200
    
    # Create an index for the graph
    index = graph.index()

    # Name tests
    assert len(index.search_nodes("name:ben")) == 1
    assert len(index.search_nodes("name:ben OR name:hamza")) == 2
    assert len(index.search_nodes("name:ben AND name:hamza")) == 0
    assert len(index.search_nodes("name: IN [ben, hamza]")) == 2

    # Property tests
    assert len(index.search_nodes("value:<120 OR value_f:>30")) == 3
    assert len(index.search_nodes("value:[0 TO 60]")) == 2
    assert len(index.search_nodes("value:[0 TO 60}")) == 1  # } == exclusive
    assert len(index.search_nodes("value:>59 AND value_str:abc123")) == 1

    # edge tests
    assert len(index.search_edges("from:ben")) == 2
    assert len(index.search_edges("from:ben OR from:haaroon")) == 3
    assert len(index.search_edges("to:haaroon AND from:ben")) == 1
    assert len(index.search_edges("to: IN [ben, hamza]")) == 2

    # edge prop tests
    assert len(index.search_edges("value:<120 OR value_f:>30")) == 3
    assert len(index.search_edges("value:[0 TO 60]")) == 2
    assert len(index.search_edges("value:[0 TO 60}")) == 1  # } == exclusive
    assert len(index.search_edges("value:>59 AND value_str:abc123")) == 1

    # Collect the out-neighbours of nodes
    out_neighbours = graph.nodes.out_neighbours.name.collect()
    out_neighbours = (set(n) for n in out_neighbours)
    out_neighbours = dict(zip(graph.nodes.name, out_neighbours))
    assert out_neighbours == {'hamza': set(), 'ben': {'haaroon', 'hamza'}, 'haaroon': {'hamza'}}
    
    # Collect the out-out-neighbours (two hops away) of nodes
    out_out_neighbours = graph.nodes.out_neighbours.out_neighbours.name.collect()
    assert out_out_neighbours == [[], ['hamza'], []]
    
    # Save the graph to a file and load it back
    tmpdirname = tempfile.TemporaryDirectory()
    graph_path = tmpdirname.name + '/graph.bin'
    graph.save_to_file(graph_path)
    
    # Add a new node to the loaded graph, save it, and load it back
    g1 = graph.load_from_file(graph_path)
    assert g1.node('ben').name == 'ben'
    g1.add_node(4, 'shivam') 
    graph_path = tmpdirname.name + "/graph2.bin"
    g1.save_to_file(graph_path)
    g2 = graph.load_from_file(graph_path)
    assert g2.node('shivam').name == 'shivam'
    
    # Create an instance of a Graph with deletions
    graph = PersistentGraph()

    # Add nodes with deletions and properties
    graph.add_node(0, 1, {"type": "wallet", "cost": 99.5})
    graph.add_node(-1, 2, {"type": "wallet", "cost": 10.0})
    graph.add_node(6, 3, {"type": "wallet", "cost": 76.0})

    edges = [(1, 1, 2, 4.0), (2, 1, 3, 4.0), (-1, 2, 1, 2.0), (0, 1, 1, 3.0), (7, 3, 2, 6.0), (1, 1, 1, 2.0)]
    
    # Add edges with deletions and properties
    for e in edges:
        graph.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test", "weight": e[3]})
    graph.edge(edges[0][1], edges[0][2]).add_constant_properties({"static": "test"})
    graph.delete_edge(10, edges[0][1], edges[0][2])
    
    # Assert that an edge has been deleted
    assert graph.edge(edges[0][1], edges[0][2]).is_deleted() == True
    
    # Find nodes with a specific properties
    nodes = graph.find_nodes({"cost": 99.5})
    assert nodes[0].properties.get('cost') == 99.5
    
    # Find edges with a specific properties
    edges = graph.find_edges({"prop1": 1})
    assert edges[0].properties.get('prop1') == 1
    
     # Collect all edges of a node
    all_edges = graph.node(1).edges
    assert all_edges.src.name.collect() == ['1', '2', '1', '1']
    
    # Run Dijkstra's algorithm for single-source shortest paths
    assert dijkstra_single_source_shortest_paths(graph, 1, [2]) == {'2': (4.0, ['1', '2'])}

if __name__ == "__main__":
    main()

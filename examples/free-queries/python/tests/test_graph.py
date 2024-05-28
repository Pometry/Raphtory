import tempfile
import pytest
from raphtory import Graph, PersistentGraph
from raphtory.algorithms import dijkstra_single_source_shortest_paths
from typing import TypeVar, Callable
import os
import time


B = TypeVar('B')

def measure(name: str, f: Callable[..., B], *args, print_result: bool = True) -> B:
    start_time = time.time()
    result = f(*args)
    elapsed_time = time.time() - start_time
    
    time_unit = "s"
    elapsed_time_display = elapsed_time
    if elapsed_time < 1:
        time_unit = "ms"
        elapsed_time_display *= 1000

    if print_result:
        print(f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}, result: {result}")
    else:
        print(f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}")

    return result


def build_graph():
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
    
    return graph
    
def build_graph_with_deletions():
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
    
    return graph
    
    
def test_graph():
    print()
    graph = measure("Build graph", build_graph, print_result=False)
    
    # Add constant properties to a node 
    measure("Add static property",  graph.node('ben').add_constant_properties, {'static prop': 123}, print_result=False)
    assert measure("Get static property", graph.node('ben').properties.get, 'static prop', print_result=False) == 123
    
    # Update/Add properties of a node
    props_t2 = {"value2": 100, "value_f": 15.0}
    measure("Update node property", graph.node("ben").add_updates, 7, props_t2, print_result=False)
    assert measure("Get temporal property", graph.node("ben").properties.get, 'value2', print_result=False) == 100

    # Update properties of an edge
    props_t2 = {
        "value": 200,
        "value_str": "I wanna spock right now",
    }
    measure("Update edge property", graph.edge('ben', 'haaroon').add_updates, 7, props_t2, print_result=False)
    assert measure("Get edge property", graph.edge('ben', 'haaroon').properties.get, 'value', print_result=False) == 200   
    
    # Create an index for the graph
    index = graph.index()
    
        # Name tests
    assert len(measure("Search nodes= name:ben", index.search_nodes, "name:ben", print_result=False)) == 1
    assert len(measure("Search nodes= name:ben OR name:hamza", index.search_nodes, "name:ben OR name:hamza", print_result=False)) == 2
    assert len(measure("Search nodes= name:ben AND name:hamza", index.search_nodes, "name:ben AND name:hamza", print_result=False)) == 0
    assert len(measure("Search nodes= name: IN [ben, hamza]", index.search_nodes, "name: IN [ben, hamza]", print_result=False)) == 2

    # Property tests
    assert len(measure("Search nodes= value:<120 OR value_f:>30", index.search_nodes, "value:<120 OR value_f:>30", print_result=False)) == 3
    assert len(measure("Search nodes= value:[0 TO 60]", index.search_nodes, "value:[0 TO 60]", print_result=False)) == 2
    assert len(measure("Search nodes= value:[0 TO 60}", index.search_nodes, "value:[0 TO 60}", print_result=False)) == 1  # } == exclusive
    assert len(measure("Search nodes= value:>59 AND value_str:abc123", index.search_nodes, "value:>59 AND value_str:abc123", print_result=False)) == 1

    # edge tests
    assert len(measure("Search nodes= from:ben", index.search_edges, "from:ben", print_result=False)) == 2
    assert len(measure("Search nodes= from:ben OR from:haaroon", index.search_edges, "from:ben OR from:haaroon", print_result=False)) == 3
    assert len(measure("Search nodes= to:haaroon AND from:ben", index.search_edges, "to:haaroon AND from:ben", print_result=False)) == 1
    assert len(measure("Search nodes= to: IN [ben, hamza]", index.search_edges, "to: IN [ben, hamza]", print_result=False)) == 2

    # edge prop tests
    assert len(measure("Search nodes= value:<120 OR value_f:>30", index.search_edges, "value:<120 OR value_f:>30", print_result=False)) == 3
    assert len(measure("Search nodes= value:[0 TO 60]", index.search_edges, "value:[0 TO 60]", print_result=False)) == 2
    assert len(measure("Search nodes= value:[0 TO 60}", index.search_edges, "value:[0 TO 60}", print_result=False)) == 1  # } == exclusive
    assert len(measure("Search nodes= value:>59 AND value_str:abc123", index.search_edges, "value:>59 AND value_str:abc123", print_result=False)) == 1
    
    # Collect the out-neighbours of nodes
    out_neighbours = measure("Get out neighbours", graph.nodes.out_neighbours.name.collect, print_result=False)
    out_neighbours = (set(n) for n in out_neighbours)
    out_neighbours = dict(zip(graph.nodes.name, out_neighbours))
    assert out_neighbours == {'hamza': set(), 'ben': {'haaroon', 'hamza'}, 'haaroon': {'hamza'}}
    
    # Collect the out-out-neighbours (two hops away) of nodes
    out_out_neighbours = measure("Get out neighbours of out neighbours", graph.nodes.out_neighbours.out_neighbours.name.collect, print_result=False)
    assert out_out_neighbours == [[], ['hamza'], []]
    
    # Save the graph to a file and load it back
    tmpdirname = tempfile.TemporaryDirectory()
    graph_path = tmpdirname.name + '/graph.bin'
    measure("Save graph to disk", graph.save_to_file, graph_path, print_result=False)   
    
    # Add a new node to the loaded graph, save it, and load it back
    g1 = measure("Load graph from disk", graph.load_from_file, graph_path, print_result=False)  
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
    
    # Find nodes with specific properties
    nodes = measure("Find nodes with specific properties", graph.find_nodes, {"cost": 99.5}, print_result=False)
    assert nodes[0].properties.get('cost') == 99.5
    
    # Find edges with specific properties
    edges = measure("Find edges with specific properties", graph.find_edges, {"prop1": 1}, print_result=False)
    assert edges[0].properties.get('prop1') == 1
    
     # Collect all edges of a node
    all_edges = graph.node(1).edges
    assert all_edges.src.name.collect() == ['1', '2', '1', '1']
        
    # Run Dijkstra's algorithm for single-source shortest paths
    assert measure("Shortest path", dijkstra_single_source_shortest_paths, graph, 1, [2], print_result=False) == {'2': (2.0, ['1', '2'])}


def test_graph_with_deletions():
    measure("Build graph with deletions", build_graph_with_deletions, print_result=False)

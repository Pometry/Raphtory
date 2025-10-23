from raphtory import Graph

def test_path_from_node():
    g: Graph = Graph()
    node_a = g.add_node(1, "A")
    g.add_node(2, "B")
    g.add_node(3, "C")
    g.add_node(4, "D")
    g.add_edge(5, "A", "B")
    g.add_edge(6, "A", "C")
    g.add_edge(7, "C", "D")
    names = node_a.neighbours.name
    assert names == ["B", "C"]
    print(f"\nnames: {names}")

    names_nested = node_a.neighbours.neighbours.name
    # Only neighbour of B is A, neighbours of C are A and D
    assert names_nested == [["A"], ["A", "D"]]
    print(f"\nnames_nested: {names_nested}")

    triple_nested = node_a.neighbours.neighbours.neighbours.name
    # should be [[["B", "C"]], [["B", "C"], ["C"]]]
    # we have   [ ["B", "C"],    ["B", "C", "C"]]
    print(f"\ntriple_nested: {triple_nested}")

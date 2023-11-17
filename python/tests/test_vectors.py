from raphtory import Graph
from raphtory.vectors import VectorisedGraph

embedding_map = {
    "node1": [1.0, 0.0, 0.0],
    "node2": [0.0, 1.0, 0.0],
    "node3": [0.0, 0.0, 1.0],
    "node4": [1.0, 1.0, 0.0],
    "edge1": [1.0, 0.1, 0.0],
    "edge2": [0.0, 1.0, 0.1],
    "edge3": [0.0, 1.0, 1.0]
}

def single_embedding(text: str):
    try:
        return embedding_map[text]
    except:
        raise Exception(f"unexpected document content: {text}")

def embedding(texts: list[str]):
    return [single_embedding(text) for text in texts]

def floats_are_equals(float1: float, float2: float) -> bool:
    return float1 + 0.001 > float2 and float1 - 0.001 < float2

def create_graph() -> VectorisedGraph:
    g = Graph()

    g.add_vertex(1, "node1", {"doc": "node1"})
    g.add_vertex(2, "node2", {"doc": "node2"})
    g.add_vertex(3, "node3", {"doc": "node3"})
    g.add_vertex(4, "node4", {"doc": "node4"})

    g.add_edge(2, "node1", "node2", {"doc": "edge1"})
    g.add_edge(3, "node1", "node3", {"doc": "edge2"})
    g.add_edge(4, "node3", "node4", {"doc": "edge3"})

    vg = VectorisedGraph.build(g, embedding, node_document="doc", edge_document="doc")

    return vg


def test_selection():
    vg = create_graph()

    assert(len(vg.get_documents()) == 0)
    assert(len(vg.get_documents_with_scores()) == 0)

    nodes_to_select = ["node1", "node2"]
    edges_to_select = [("node1", "node2"), ("node1", "node3")]

    nodes = vg.select_nodes(nodes_to_select).nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == nodes_to_select)

    edges = vg.select_edges(edges_to_select).edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == edges_to_select)

    edge_tuples = [(edge.src, edge.dst) for edge in edges]
    selection = vg.select(nodes, edge_tuples)
    nodes_returned = selection.nodes()
    assert(nodes == nodes_returned)
    edges_returned = selection.edges()
    assert(edges == edges_returned)


def test_search():
    vg = create_graph()

    assert(len(vg.search_similar_edges("edge1", 10).nodes()) == 0)
    assert(len(vg.search_similar_nodes("node1", 10).edges()) == 0)

    nodes = vg.search_similar_nodes([1.0, 0.0, 0.0], 1).nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == ["node1"])

    edges = vg.search_similar_edges([1.0, 0.0, 0.0], 1).edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == [("node1", "node2")])

    doc1_with_score, doc2_with_score = vg.search_similar_entities([1.0, 0.0, 0.0], 2).get_documents_with_scores()
    doc1, score1 = doc1_with_score
    doc2, score2 = doc2_with_score
    assert(floats_are_equals(score1, 1.0))
    assert(doc1.entity.name == "node1")
    assert(doc1.content == "node1")
    assert((doc2.entity.src.name, doc2.entity.dst.name) == ("node1", "node2"))

    docs = vg.search_similar_entities([0.0, 0.0, 1.1], 3).get_documents()
    doc_contents = [doc.content for doc in docs]
    assert(doc_contents == ["node3", "edge3", "edge2"])

    # chained search
    docs = vg.search_similar_nodes("node2", 1).search_similar_edges("node3", 1).search_similar_entities("node1", 2).get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node2", "edge3", "node1", "edge1"])

def test_expansion():
    vg = create_graph()

    selection = vg.search_similar_entities("node1", 1).expand(2)
    assert(len(selection.get_documents()) == 5)
    assert(len(selection.nodes()) == 3)
    assert(len(selection.edges()) == 2)

    selection = vg.search_similar_entities("node1", 1).expand_with_search("edge1", 1).expand_with_search("node2", 1)
    assert(len(selection.get_documents()) == 3)
    nodes = selection.nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == ["node1", "node2"])
    edges = selection.edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == [("node1", "node2")])

    selection = vg.expand_with_search("node3", 10)
    assert(len(selection.get_documents()) == 0)

    selection = vg.search_similar_entities("node1", 1).expand_with_search("node3", 10)
    assert(len(selection.get_documents()) == 7)
    assert(len(selection.nodes()) == 4)
    assert(len(selection.edges()) == 3)

def test_windows():
    vg = create_graph()

    selection1 = vg.search_similar_nodes("node1", 1, (4, 5))
    docs = selection1.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4"])

    selection2 = selection1.search_similar_nodes("node4", 1, (1, 2))
    docs = selection2.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4", "node1"])

    selection3 = selection2.expand(10, (0, 3))
    docs = selection3.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4", "node1", "edge1", "node2"])

    selection4 = selection3.expand_with_search("edge2", 100, (0, 4))
    docs = selection4.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4", "node1", "edge1", "node2", "edge2", "node3"])

    selection5 = selection4.expand_with_search("node1", 100, (20, 100))
    docs = selection5.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4", "node1", "edge1", "node2", "edge2", "node3"])

    selection5 = selection4.expand(10, (4, 100))
    docs = selection5.get_documents()
    contents = [doc.content for doc in docs]
    assert(contents == ["node4", "node1", "edge1", "node2", "edge2", "node3", "edge3"])

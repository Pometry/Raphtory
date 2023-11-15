from raphtory import Graph
from raphtory.vectors import VectorizedGraph

def single_embedding(text: str) -> list[float]:
    match text:
        case "node1": return [1.0, 0.0, 0.0]
        case "node2": return [0.0, 1.0, 0.0]
        case "node3": return [0.0, 0.0, 1.0]
        case "node4": return [1.0, 1.0, 0.0]
        case "edge1": return [1.0, 0.1, 0.0]
        case "edge2": return [0.0, 1.0, 0.1]
        case "edge3": return [0.0, 1.0, 1.0]
        case _: raise Exception(f"unexpected document content: {text}")

def embedding(texts: list[str]) -> list[list[float]]:
    return [single_embedding(text) for text in texts]

def floats_are_equals(float1: float, float2: float) -> bool:
    return float1 + 0.001 > float2 and float1 - 0.001 < float2

def create_graph() -> VectorizedGraph:
    g = Graph()

    g.add_vertex(1, "node1", {"doc": "node1"})
    g.add_vertex(2, "node2", {"doc": "node2"})
    g.add_vertex(3, "node3", {"doc": "node3"})
    g.add_vertex(4, "node4", {"doc": "node4"})

    g.add_edge(1, "node1", "node2", {"doc": "edge1"})
    g.add_edge(1, "node1", "node3", {"doc": "edge2"})
    g.add_edge(1, "node3", "node4", {"doc": "edge3"})

    v = VectorizedGraph.build_from_graph(g, embedding, node_document="doc", edge_document="doc")

    return v


def test_selection():
    v = create_graph()

    assert(len(v.empty_selection().get_documents()) == 0)
    assert(len(v.empty_selection().get_documents_with_scores()) == 0)

    nodes_to_select = ["node1", "node2"]
    edges_to_select = [("node1", "node2"), ("node1", "node3")]

    nodes = v.select_nodes(nodes_to_select).nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == nodes_to_select)

    edges = v.select_edges(edges_to_select).edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == edges_to_select)

    edge_tuples = [(edge.src, edge.dst) for edge in edges]
    selection = v.select(nodes, edge_tuples)
    nodes_returned = selection.nodes()
    assert(nodes == nodes_returned)
    edges_returned = selection.edges()
    assert(edges == edges_returned)


def test_search():
    v = create_graph()

    assert(len(v.search_similar_edges("edge1", 10).nodes()) == 0)
    assert(len(v.search_similar_nodes("node1", 10).edges()) == 0)

    nodes = v.empty_selection().add_new_nodes([1.0, 0.0, 0.0], 1).nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == ["node1"])

    nodes = v.search_similar_nodes([1.0, 0.0, 0.0], 1).nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == ["node1"])

    edges = v.empty_selection().add_new_edges([1.0, 0.0, 0.0], 1).edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == [("node1", "node2")])

    edges = v.search_similar_edges([1.0, 0.0, 0.0], 1).edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == [("node1", "node2")])

    doc1_with_score, doc2_with_score = v.empty_selection().add_new_entities([1.0, 0.0, 0.0], 2).get_documents_with_scores()
    doc1, score1 = doc1_with_score
    doc2, score2 = doc2_with_score
    assert(floats_are_equals(score1, 1.0))
    assert(doc1.entity.name == "node1")
    assert(doc1.content == "node1")
    assert((doc2.entity.src.name, doc2.entity.dst.name) == ("node1", "node2"))

    doc1_with_score, doc2_with_score = v.search_similar_entities([1.0, 0.0, 0.0], 2).get_documents_with_scores()
    doc1, score1 = doc1_with_score
    doc2, score2 = doc2_with_score
    assert(floats_are_equals(score1, 1.0))
    assert(doc1.entity.name == "node1")
    assert(doc1.content == "node1")
    assert((doc2.entity.src.name, doc2.entity.dst.name) == ("node1", "node2"))

    docs = v.search_similar_entities([0.0, 0.0, 1.1], 3).get_documents()
    doc_contents = [doc.content for doc in docs]
    assert(doc_contents == ["node3", "edge3", "edge2"])

def test_expansion():
    v = create_graph()

    selection = v.search_similar_entities("node1", 1).expand(2)
    assert(len(selection.get_documents()) == 5)
    assert(len(selection.nodes()) == 3)
    assert(len(selection.edges()) == 2)

    selection = v.search_similar_entities("node1", 1).expand_with_search("edge1", 1).expand_with_search("node2", 1)
    assert(len(selection.get_documents()) == 3)
    nodes = selection.nodes()
    node_names_returned = [node.name for node in nodes]
    assert(node_names_returned == ["node1", "node2"])
    edges = selection.edges()
    edge_names_returned = [(edge.src.name, edge.dst.name) for edge in edges]
    assert(edge_names_returned == [("node1", "node2")])

    selection = v.empty_selection().expand_with_search("node3", 10)
    assert(len(selection.get_documents()) == 0)

    selection = v.search_similar_entities("node1", 1).expand_with_search("node3", 10)
    assert(len(selection.get_documents()) == 7)
    assert(len(selection.nodes()) == 4)
    assert(len(selection.edges()) == 3)

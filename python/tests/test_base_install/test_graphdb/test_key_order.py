import tempfile

import pytest
from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer

# Twenty keys, so a map that does not keep insertion order is all but certain to show it.
KEYS = [f"k{i}" for i in range(20)]
VALUES = {key: i for i, key in enumerate(KEYS)}


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_property_keys_keep_the_dict_order(graph_type):
    g = graph_type()
    g.add_node(1, "added", properties=VALUES)
    g.create_node(1, "created", properties=VALUES)
    g.add_edge(1, "a", "b", properties=VALUES)
    g.add_node(1, "updated").add_updates(2, VALUES)
    g.add_properties(1, VALUES)

    assert g.node("added").properties.keys() == KEYS
    assert g.node("created").properties.keys() == KEYS
    assert g.edge("a", "b").properties.keys() == KEYS
    assert g.node("updated").properties.keys() == KEYS
    assert g.properties.keys() == KEYS


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_metadata_keys_keep_the_dict_order(graph_type):
    g = graph_type()
    g.add_metadata(VALUES)
    g.add_node(1, "n").add_metadata(VALUES)
    g.add_edge(1, "a", "b").add_metadata(VALUES)
    g.add_node(1, "u").update_metadata(VALUES)

    assert g.metadata.keys() == KEYS
    assert g.node("n").metadata.keys() == KEYS
    assert g.edge("a", "b").metadata.keys() == KEYS
    assert g.node("u").metadata.keys() == KEYS


def test_remote_writes_keep_the_dict_order():
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            client = server.get_client()
            client.new_graph("g", "EVENT")
            rg = client.remote_graph("g")
            rg.add_node(1, "n", properties=VALUES)
            rg.add_edge(1, "a", "b", properties=VALUES)
            rg.add_metadata(VALUES)
            rg.node("n").add_metadata(VALUES)

            g = client.receive_graph("g")
            assert g.node("n").properties.keys() == KEYS
            assert g.edge("a", "b").properties.keys() == KEYS
            assert g.metadata.keys() == KEYS
            assert g.node("n").metadata.keys() == KEYS

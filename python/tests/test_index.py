from raphtory import Graph

# Range queries are not supported for properties in tantivy (as of 0.22) because they are implemented as json.
# See https://github.com/quickwit-oss/tantivy/issues/1709

def test_search_in_python():
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2,
        "ben",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_node(
        3,
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )

    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        1,
        "ben",
        "hamza",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_edge(
        3,
        "ben",
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )

    # Name tests
    assert len(g.search_nodes("name:ben")) == 1
    assert len(g.search_nodes("name:ben OR name:hamza")) == 2
    assert len(g.search_nodes("name:ben AND name:hamza")) == 0
    assert len(g.search_nodes("name: IN [ben, hamza]")) == 2

    # Property tests
    # assert len(g.search_nodes("value:<120 OR value_f:>30")) == 3
    assert len(g.search_nodes("value:199")) == 1
    # assert len(g.search_nodes("value:[0 TO 60]")) == 2
    # assert len(g.search_nodes("value:[0 TO 60}")) == 1  # } == exclusive
    # assert len(g.search_nodes("value:>59 AND value_str:abc123")) == 1

    # edge tests
    assert len(g.search_edges("from:ben")) == 2
    assert len(g.search_edges("from:ben OR from:haaroon")) == 3
    assert len(g.search_edges("to:haaroon AND from:ben")) == 1
    assert len(g.search_edges("to: IN [ben, hamza]")) == 2

    # edge prop tests
    # assert len(g.search_edges("value:<120 OR value_f:>30")) == 3
    # assert len(g.search_edges("value:[0 TO 60]")) == 2
    # assert len(g.search_edges("value:[0 TO 60}")) == 1  # } == exclusive
    # assert len(g.search_edges("value:>59 AND value_str:abc123")) == 1

    # Multiple history points test
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2, "hamza", properties={"value": 70, "value_f": 21.3, "value_str": "avc125"}
    )
    g.add_node(
        3, "hamza", properties={"value": 80, "value_f": 11.3, "value_str": "dsc2312"}
    )

    # The semantics here are that the expressions independently need to evaluate at ANY point in the lifetime of the node - hence hamza is returned even though at no point does he have both these values at the same time
    # assert len(g.search_nodes("value:<70 AND value_f:<19.2")) == 1

    g.add_node(
        4, "hamza", properties={"value": 100, "value_f": 11.3, "value_str": "dsc2312"}
    )
    # the graph isn't currently reindexed so this will not return hamza even though he now has a value which fits the bill
    # assert len(g.search_nodes("value:>99")) == 0


def test_type_search():
    g = Graph()
    ben = g.add_node(1, "ben", node_type="type_1")
    hamza = g.add_node(2, "hamza", node_type="type_2")
    assert g.search_nodes("node_type:type_1") == [ben]
    assert set(g.search_nodes("node_type:type_1 OR node_type:type_2")) == {
        hamza,
        ben,
    }


def test_search_with_windows():
    # Window test
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2, "hamza", properties={"value": 70, "value_f": 21.3, "value_str": "avc125"}
    )
    g.add_node(
        3, "hamza", properties={"value": 80, "value_f": 11.3, "value_str": "dsc2312"}
    )

    g.add_edge(
        1,
        "haaroon",
        "hamza",
        properties={"value": 50, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 21.3, "value_str": "abddasc1223"},
    )
    g.add_edge(
        3,
        "haaroon",
        "hamza",
        properties={"value": 70, "value_f": 11.3, "value_str": "abdsda2c123"},
    )
    g.add_edge(
        4,
        "ben",
        "naomi",
        properties={"value": 100, "value_f": 22.3, "value_str": "ddddd"},
    )

    w_g = g.window(1, 3)


    # Testing if windowing works - ben shouldn't be included and Hamza should only have max value of 70
    assert len(w_g.search_nodes("name:ben")) == 0
    assert len(w_g.search_nodes("value:70")) == 1
    # assert len(w_index.search_nodes("value:>80")) == 0

    assert len(w_g.search_edges("from:ben")) == 0
    # assert len(w_index.search_edges("from:haaroon AND value:>70")) == 0
    assert len(w_g.search_edges("from:haaroon AND to:hamza")) == 1


def test_search_with_subgraphs():
    g = Graph()
    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        1,
        "ben",
        "hamza",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_edge(
        3,
        "ben",
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )
    g.add_edge(4, "hamza", "naomi")

    assert len(g.search_edges("from:hamza OR to:hamza")) == 3

    subgraph = g.subgraph([g.node("ben"), g.node("hamza"), g.node("haaroon")])

    assert len(subgraph.search_edges("from:hamza OR to:hamza")) == 2

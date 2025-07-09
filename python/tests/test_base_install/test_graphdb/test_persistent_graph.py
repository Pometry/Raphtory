from raphtory import PersistentGraph, Graph


def test_basics():
    G = PersistentGraph()
    G.add_edge(1, "Alice", "Bob")
    G.add_edge(3, "Bob", "Charlie")
    G.delete_edge(5, "Alice", "Bob")
    G.add_edge(10, "Alice", "Bob")
    assert G.count_edges() == 2


def test_hanging_edges():
    G = PersistentGraph()
    G.delete_edge(5, "Alice", "Bob")
    assert G.count_edges() == 1
    assert G.at(6).count_edges() == 0
    assert G.latest_time == 5
    assert G.at(G.latest_time).count_edges() == 0
    assert G.at(G.latest_time - 1).count_edges() == 0


def test_overlapping_times():
    G = PersistentGraph()
    G.add_edge(1, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    G.add_edge(3, "Alice", "Bob")
    G.delete_edge(7, "Alice", "Bob")
    exploded = G.edges.explode()
    assert list(zip(exploded.earliest_time, exploded.latest_time)) == [(1, 3), (3, 5)]

    G.add_edge(1, "Alice", "Bob", layer="colleagues")
    G.delete_edge(5, "Alice", "Bob", layer="colleagues")
    G.add_edge(3, "Alice", "Bob", layer="friends")
    G.delete_edge(7, "Alice", "Bob", layer="friends")
    exploded = G.layers(["colleagues", "friends"]).edges.explode()
    assert list(zip(exploded.earliest_time, exploded.latest_time)) == [(1, 5), (3, 7)]


def test_node_updates_at_same_time():
    g = PersistentGraph()

    g.add_node(1, 1, properties={"prop1": 1})  # false
    g.add_node(2, 1, properties={"prop1": 2})  # true
    g.add_node(2, 1, properties={"prop1": 3})  # true
    g.add_node(8, 1, properties={"prop1": 4})  # false
    g.add_node(9, 1, properties={"prop1": 5})  # false

    print(g.window(2, 10).node(1).properties.temporal.get("prop1").values())


def test_same_time_op():
    G1 = PersistentGraph()
    G1.add_edge(1, 1, 2, properties={"message": "hi"})
    G1.delete_edge(1, 1, 2)
    G2 = PersistentGraph()
    G2.delete_edge(1, 1, 2)
    G2.add_edge(1, 1, 2, properties={"message": "hi"})
    exploded_1 = G1.edges.explode()
    exploded_2 = G2.edges.explode()
    assert list(zip(exploded_1.earliest_time, exploded_1.latest_time)) == [(1, 1)]
    assert list(zip(exploded_2.earliest_time, exploded_2.latest_time)) == [
        (1, 1),
    ]
    # added then deleted means edge does not exist at 1
    assert G1.at(1).count_temporal_edges() == 0
    # deleted then added means update is included
    assert G2.at(1).count_temporal_edges() == 1


def test_at_boundaries():
    G = PersistentGraph()
    G.add_edge(2, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    # before the edge is added
    assert G.at(0).count_nodes() == 0
    assert G.at(0).count_edges() == 0

    assert G.at(1).count_nodes() == 0
    assert G.at(1).count_edges() == 0

    assert G.at(2).count_nodes() == 2
    assert G.at(2).count_edges() == 1

    assert G.at(3).count_nodes() == 2
    assert G.at(3).count_edges() == 1

    assert G.at(4).count_nodes() == 2
    assert G.at(4).count_edges() == 1

    assert (
        G.at(5).count_nodes() == 0
    )  # nodes are deleted as they were only brought in by the edge
    assert G.at(5).count_edges() == 0

    assert G.at(6).count_nodes() == 0
    assert G.at(6).count_edges() == 0


def test_before_boundaries():
    G = PersistentGraph()
    G.add_edge(2, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    assert G.before(0).count_nodes() == 0
    assert G.before(0).count_edges() == 0

    assert G.before(1).count_nodes() == 0
    assert G.before(1).count_edges() == 0

    assert G.before(2).count_nodes() == 0
    assert G.before(2).count_edges() == 0

    assert G.before(3).count_nodes() == 2
    assert G.before(3).count_edges() == 1

    assert G.before(4).count_nodes() == 2
    assert G.before(4).count_edges() == 1

    assert G.before(5).count_nodes() == 2
    assert G.before(5).count_edges() == 1

    assert G.before(6).count_nodes() == 2
    assert G.before(6).count_edges() == 1

    assert G.before(5).edge("Alice", "Bob").is_valid() == True
    assert G.before(6).edge("Alice", "Bob").is_valid() == False


def test_after_boundaries():
    G = PersistentGraph()
    G.add_edge(2, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    assert G.after(0).count_nodes() == 2
    assert G.after(0).count_edges() == 1

    assert G.after(1).count_nodes() == 2
    assert G.after(1).count_edges() == 1

    assert G.after(2).count_nodes() == 2
    assert G.after(2).count_edges() == 1

    assert G.after(3).count_nodes() == 2
    assert G.after(3).count_edges() == 1

    assert G.after(4).count_nodes() == 0
    assert G.after(4).count_edges() == 0

    assert G.after(5).count_nodes() == 0
    assert G.after(5).count_edges() == 0

    assert G.after(6).count_nodes() == 0
    assert G.after(6).count_edges() == 0


def test_window_boundaries():
    G = PersistentGraph()
    G.add_edge(2, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")

    assert G.window(0, 2).count_nodes() == 0
    assert G.window(0, 2).count_edges() == 0

    assert G.window(0, 4).count_nodes() == 2
    assert G.window(0, 4).count_edges() == 1

    assert G.window(3, 4).count_nodes() == 2
    assert G.window(3, 4).count_edges() == 1

    assert G.window(5, 8).count_nodes() == 0
    assert G.window(5, 8).count_edges() == 0

    assert G.window(1, 8).count_nodes() == 2
    assert G.window(1, 8).count_edges() == 1

    assert G.window(6, 10).count_nodes() == 0
    assert G.window(6, 10).count_edges() == 0


def test_graph_type_swap():
    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(30, 1, 4)
    eg = Graph()
    eg.add_edge(1, 1, 2)
    eg.add_edge(2, 1, 3)
    eg.add_edge(30, 1, 4)
    assert type(g.persistent_graph()) == PersistentGraph
    assert type(g.event_graph()) == Graph
    assert g.persistent_graph().at(15).count_edges() == 2
    assert g.event_graph().at(2).count_edges() == 1

    assert type(eg.persistent_graph()) == PersistentGraph
    assert type(eg.event_graph()) == Graph
    assert eg.persistent_graph().count_edges() == 3
    assert eg.persistent_graph().at(15).count_edges() == 2
    assert eg.event_graph().at(2).count_edges() == 1


from raphtory import PersistentGraph
from raphtory import filter
from raphtory import Prop


def test_basic_valid():
    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.delete_edge(10, 1, 3)

    g.add_edge(30, 1, 4)

    assert g.count_edges() == 3
    assert g.valid().count_edges() == 2
    assert g.at(9).valid().count_edges() == 2


def test_properties_valid():
    g = PersistentGraph()
    g.add_edge(1, 1, 2, layer="red", properties={"weight": 1})
    g.add_edge(2, 1, 2, layer="green", properties={"weight": 2})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3})
    g.delete_edge(4, 1, 2, layer="red")

    g2 = PersistentGraph()
    g2.add_edge(1, 1, 2, layer="red", properties={"weight": 1})
    g2.delete_edge(4, 1, 2, layer="red")

    assert (
        g2.edge(1, 2).properties.get("weight")
    ) == 1  # Need to think about if this is correct
    assert (
        g.edge(1, 2).properties.get("weight")
    ) == 3  # Need to think about if this is correct

    assert list(g.edge(1, 2).properties.temporal.get("weight").values()) == [1, 2, 3]
    assert list(g.valid().edge(1, 2).properties.temporal.get("weight").values()) == [
        2,
        3,
    ]

    assert g.edge(1, 2).is_deleted() == False
    assert g.valid().count_edges() == 1
    assert g.layer("red").edge(1, 2).is_deleted() == True
    assert g.layer("red").valid().count_edges() == False
    g.delete_edge(5, 1, 2, layer="green")
    assert g.edge(1, 2).is_deleted() == False
    assert g.valid().count_edges() == 1
    g.delete_edge(6, 1, 2, layer="blue")
    assert g.edge(1, 2).is_deleted() == True
    assert g.valid().count_edges() == 0


def test_filtering_valid():
    g = PersistentGraph()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1})
    g.add_edge(2, 1, 2, layer="blue", properties={"weight": 2})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1})
    g.add_edge(2, 1, 3, layer="blue", properties={"weight": 2})
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3})

    f = filter.Property("weight").temporal().latest() < 3
    e = g.filter_edges(f).edge(1, 2)
    assert e == None
    # assert list(e.properties.temporal.get("weight").values) == [1,2] -- this would be the case for filter_edge_layer?

    # f = filter.Property("weight") < 3
    f = Prop("weight") < 3  # this needs to be converted to the above
    e = (
        g.valid().filter_exploded_edges(f).edge(1, 2)
    )  # this returns an edge which is invalid -- this would not be the case with Shivams changes
    assert e.is_valid() == False  # latest update is now a deletion
    assert list(e.properties.temporal.get("weight").values()) == [
        1,
        2,
    ]  # property value gone (makes sense)

    e2 = g.valid().filter_exploded_edges(f).edge(1, 3)
    assert e2.is_valid() == True  # dead in red but not blue
    assert list(e.properties.temporal.get("weight").values()) == [
        1,
        2,
    ]  # property values the same

    f = Prop("weight") > 5  # this needs to be converted to the above
    e = g.filter_exploded_edges(f).edge(
        1, 2
    )  # this should be probably be filtered out (along with edges that only have deletions) as we discussed
    assert list(e.properties.temporal.get("weight").values()) == []


# TODO this will not pass until we fix hanging edges
# def test_window_boundaries_same_time_op():
#     G = PersistentGraph()
#     G.add_edge(2, "Alice", "Bob")
#     G.delete_edge(2, "Alice", "Bob")
#
#     assert G.window(0, 2).count_nodes() == 0
#     assert G.window(0, 2).count_edges() == 0
#
#     assert G.window(2, 3).count_nodes() == 2
#     assert G.window(2, 3).count_edges() == 1
#
#     assert G.window(3, 10).count_nodes() == 2
#     assert G.window(3, 10).count_edges() == 0
#
#
#     G = PersistentGraph()
#     G.delete_edge(2, "Alice", "Bob")
#     G.add_edge(2, "Alice", "Bob")
#     print(G.window(0, 2).count_nodes(),G.window(0, 2).count_edges())
#
#     assert G.window(0, 2).count_nodes() == 0
#     assert G.window(0, 2).count_edges() == 0
#
#     assert G.window(2, 3).count_nodes() == 2
#     assert G.window(2, 3).count_edges() == 1
#
#     assert G.window(3, 10).count_nodes() == 2
#     assert G.window(3, 10).count_edges() == 0

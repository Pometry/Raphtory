from raphtory import PersistentGraph


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
    assert G.at(G.latest_time - 1).count_edges() == 1


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
        (-9223372036854775808, 1),
        (1, 9223372036854775807),
    ]
    assert G1.at(1).count_temporal_edges() == 1
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

    assert G.at(5).count_nodes() == 2
    assert G.at(5).count_edges() == 0

    assert G.at(6).count_nodes() == 2
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

    assert G.after(4).count_nodes() == 2
    assert G.after(4).count_edges() == 0

    assert G.after(5).count_nodes() == 2
    assert G.after(5).count_edges() == 0

    assert G.after(6).count_nodes() == 2
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

    assert G.window(5, 8).count_nodes() == 2
    assert G.window(5, 8).count_edges() == 0

    assert G.window(1, 8).count_nodes() == 2
    assert G.window(1, 8).count_edges() == 1

    assert G.window(6, 10).count_nodes() == 2
    assert G.window(6, 10).count_edges() == 0


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

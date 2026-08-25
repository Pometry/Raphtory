"""Node ids keep their type over the wire: integer-indexed graphs report ints.

A graph is either string-indexed or integer-indexed, decided by its first node
write, and the id type is part of the answer: locally ``node(5).id`` is the
integer ``5``, not ``"5"``. Nothing else in this suite writes an integer id, so
every id assertion elsewhere is a string-indexed one and a client that
stringified ids would pass the entire matrix.

It did. The client sent every id to the server as a string, so the server built
a *string-indexed* graph and the divergence was twofold: ids read back as
``"5"`` instead of ``5``, and the graph's own id-type enforcement disappeared —
a string id written into an integer graph is refused locally and was accepted
remotely.

Both halves are asserted here: the ids that come back, and the rejection that
should happen. ``name`` is checked alongside because it is *not* affected — an
integer node's name is its decimal string on both sides — so a fix that turned
every id into an int everywhere would fail here too.
"""

import pytest

from _parity import GRAPH_TYPES, assert_parity, graph_pair

# Integer-indexed: every id is a non-negative int (`GID::U64`, so no negatives).
_INT_EDGES = [(3, 5, 3), (4, 3, 7), (5, 7, 5)]


def _build_int(g):
    for node in (5, 3, 7):
        g.add_node(1, node)
    for t, src, dst in _INT_EDGES:
        g.add_edge(t, src, dst)


@pytest.fixture(scope="module")
def int_pair():
    with graph_pair(_build_int) as pair:
        yield pair


# Every place an id surfaces, keyed by name so a failure says which one. Sorted
# where the collection's order is unspecified, so only the *values and types*
# are under test here (ordering has its own coverage).
ID_READERS = {
    "node.id": lambda g: g.node(5).id,
    "edge.id": lambda g: g.edge(5, 3).id,
    "nodes.id": lambda g: sorted(g.nodes.id),
    "edges.id": lambda g: sorted(g.edges.id),
    "neighbours.id": lambda g: sorted(g.node(5).neighbours.id),
    "nested.id": lambda g: sorted(sorted(row) for row in g.nodes.neighbours.id),
    "subgraph.nodes.id": lambda g: sorted(n.id for n in g.subgraph([5, 3]).nodes),
    "collect().id": lambda g: sorted(n.id for n in g.nodes),
    "edge.src.id": lambda g: g.edge(5, 3).src.id,
    "edge.dst.id": lambda g: g.edge(5, 3).dst.id,
}


@pytest.mark.parametrize("reader", sorted(ID_READERS))
def test_integer_ids_survive_the_round_trip(int_pair, reader):
    """Local and remote agree — which means the type agrees, since ``5`` and
    ``"5"`` are not equal in Python and the comparator coerces neither."""
    assert_parity(int_pair, ID_READERS[reader])


@pytest.mark.parametrize("reader", sorted(ID_READERS))
def test_integer_ids_are_really_integers(int_pair, reader):
    """Anti-vacuity: parity alone would also pass if *both* sides stringified.

    Asserted per side against the type rather than across sides, so a
    symmetric regression (client and server agreeing on the wrong answer)
    still fails.
    """
    for side_name, side in (("local", int_pair.local), ("remote", int_pair.remote)):
        value = ID_READERS[reader](side)
        flat = _flatten(value)
        assert flat, f"{side_name} {reader}: nothing to check"
        assert all(isinstance(x, int) for x in flat), (
            f"{side_name} {reader} returned {flat!r}; an integer-indexed graph "
            f"must report integer ids, not their string forms"
        )


def _flatten(value):
    """Every scalar in a nested id result, so one assertion covers all shapes."""
    if isinstance(value, (str, bytes)) or not hasattr(value, "__iter__"):
        return [value]
    return [x for item in value for x in _flatten(item)]


def test_names_stay_strings(int_pair):
    """`name` is the id's *string* form even on an integer-indexed graph, so a
    fix that made ids int everywhere would break this."""
    assert_parity(int_pair, lambda g: sorted(g.nodes.name))
    for side in (int_pair.local, int_pair.remote):
        assert sorted(side.nodes.name) == ["3", "5", "7"]


@pytest.mark.parametrize("graph_type", GRAPH_TYPES)
def test_a_string_id_is_refused_by_an_integer_graph(graph_type):
    """The id type is pinned by the first write, and the mismatch is refused.

    This is the half that a value-parity check cannot see: the remote used to
    accept the write, because stringifying every id meant its graph had never
    been integer-indexed in the first place. Exception parity is the assertion,
    so both sides must refuse it the same way.
    """
    with graph_pair(_build_int, graph_type=graph_type) as pair:
        assert_parity(pair, lambda g: g.add_node(9, "not-an-int"))
        # ...and the refusal left nothing behind on either side.
        assert_parity(pair, lambda g: sorted(n.id for n in g.nodes))


@pytest.mark.parametrize("graph_type", GRAPH_TYPES)
def test_an_integer_id_is_refused_by_a_string_graph(graph_type):
    """The mirror case, so the rule is pinned in both directions rather than
    just the one the bug happened to expose."""

    def build_str(g):
        g.add_node(1, "a")
        g.add_edge(2, "a", "b")

    with graph_pair(build_str, graph_type=graph_type) as pair:
        assert_parity(pair, lambda g: g.add_node(9, 42))
        assert_parity(pair, lambda g: sorted(n.id for n in g.nodes))


@pytest.mark.parametrize("graph_type", GRAPH_TYPES)
def test_integer_ids_address_the_same_entities_on_both_sides(graph_type):
    """Lookups by integer id resolve, and a missing integer id is missing on
    both sides — the addressing path, not just the reading path."""
    with graph_pair(_build_int, graph_type=graph_type) as pair:
        assert_parity(pair, lambda g: g.has_node(5))
        assert_parity(pair, lambda g: g.has_node(404))
        assert_parity(pair, lambda g: g.has_edge(5, 3))
        assert_parity(pair, lambda g: g.has_edge(3, 5))
        assert_parity(pair, lambda g: g.node(404))
        assert_parity(pair, lambda g: g.node(5).degree())

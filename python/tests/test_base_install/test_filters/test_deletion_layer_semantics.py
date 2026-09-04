"""Where an edge's deletion is recorded, relative to where the edge lives.

`delete_edge` records a deletion on one layer, creating that layer if the edge
was never added to it — so an unlayered delete of a layered edge tombstones
`_default` and leaves the original layer untouched. That state is deliberate (it
is what lets an out-of-order stream record a deletion before its addition
arrives), which makes "is this edge deleted?" a question about *aggregating*
layers rather than about any single one.

These tests pin that aggregation:

* the `is_deleted` filter and `EdgeView.is_deleted` must answer identically —
  the filter is the collection-level spelling of the method, not a different
  predicate;
* on a `PersistentGraph` the two predicates must partition, since there an edge
  is either currently alive or currently deleted.

`Graph` is covered too, where `is_valid`/`is_deleted` are independent facts
about the history (an edge can have both an addition and a deletion) rather than
a partition, so only the filter/method agreement is asserted.
"""

import pytest
from raphtory import Graph, PersistentGraph, filter

Edge = filter.Edge

# (label, add layer, delete layer) — the three ways a deletion can sit relative
# to the layer holding the edge. The last two are the ones that used to diverge.
LAYOUTS = [
    ("same layer", "work", "work"),
    ("different layer", "work", "other"),
    ("unlayered delete of a layered edge", "work", None),
]

GRAPH_TYPES = [Graph, PersistentGraph]


def _build(cls, add_layer, delete_layer):
    g = cls()
    g.add_edge(10, "b", "c", layer=add_layer)
    if delete_layer is None:
        g.delete_edge(18, "b", "c")
    else:
        g.delete_edge(18, "b", "c", layer=delete_layer)
    # An unrelated later edge, so the view's end is past the deletion and
    # "currently deleted" is a meaningful question.
    g.add_edge(25, "a", "d")
    return g


def _ids(collection):
    return {e.id for e in collection}


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "label,add_layer,delete_layer", LAYOUTS, ids=[c[0] for c in LAYOUTS]
)
def test_is_deleted_filter_agrees_with_the_method(cls, label, add_layer, delete_layer):
    g = _build(cls, add_layer, delete_layer)
    selected = _ids(g.edges[Edge.is_deleted()])
    for edge in g.edges:
        assert (edge.id in selected) == edge.is_deleted(), (
            f"{label}: edges[is_deleted()] and is_deleted() disagree about {edge.id}: "
            f"filter says {edge.id in selected}, method says {edge.is_deleted()}"
        )


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "label,add_layer,delete_layer", LAYOUTS, ids=[c[0] for c in LAYOUTS]
)
def test_is_valid_filter_agrees_with_the_method(cls, label, add_layer, delete_layer):
    g = _build(cls, add_layer, delete_layer)
    selected = _ids(g.edges[Edge.is_valid()])
    for edge in g.edges:
        assert (
            edge.id in selected
        ) == edge.is_valid(), (
            f"{label}: edges[is_valid()] and is_valid() disagree about {edge.id}"
        )


@pytest.mark.parametrize(
    "label,add_layer,delete_layer", LAYOUTS, ids=[c[0] for c in LAYOUTS]
)
def test_persistent_valid_and_deleted_partition(label, add_layer, delete_layer):
    """On a persistent graph every edge is either currently alive or deleted."""
    g = _build(PersistentGraph, add_layer, delete_layer)
    valid = _ids(g.edges[Edge.is_valid()])
    deleted = _ids(g.edges[Edge.is_deleted()])
    every = _ids(g.edges)
    assert (
        valid & deleted == set()
    ), f"{label}: {sorted(valid & deleted)} are both valid and deleted"
    assert (
        valid | deleted == every
    ), f"{label}: {sorted(every - (valid | deleted))} are neither valid nor deleted"


def test_edge_alive_on_another_layer_is_not_deleted():
    """The case the aggregation exists for, asserted on its own.

    The edge lives on `work` and is never deleted there; the unlayered delete
    tombstones `_default`. Alive on one layer means not deleted, so neither the
    method nor the filter may report it as deleted.
    """
    g = _build(PersistentGraph, "work", None)
    bc = g.edge("b", "c")
    # The state the assertions are about: the layers genuinely disagree.
    assert sorted(bc.layer_names) == ["_default", "work"]
    assert g.layer("work").edge("b", "c").is_valid()
    assert g.layer("_default").edge("b", "c").is_deleted()

    assert not bc.is_deleted()
    assert bc.is_valid()
    assert bc.id not in _ids(g.edges[Edge.is_deleted()])
    assert bc.id in _ids(g.edges[Edge.is_valid()])


def test_deletion_on_the_only_layer_is_deleted():
    """The complement of the case above, so the fix cannot pass by never
    reporting a deletion at all."""
    g = _build(PersistentGraph, "work", "work")
    bc = g.edge("b", "c")
    assert bc.is_deleted()
    assert not bc.is_valid()
    assert bc.id in _ids(g.edges[Edge.is_deleted()])
    assert bc.id not in _ids(g.edges[Edge.is_valid()])


def test_layered_view_scopes_the_aggregation():
    """Under `layer(...)` the aggregation is over that layer only, so the same
    edge answers differently depending on which layer is in view."""
    g = _build(PersistentGraph, "work", None)
    assert g.layer("work").edge("b", "c").id not in _ids(
        g.layer("work").edges[Edge.is_deleted()]
    )
    assert g.layer("_default").edge("b", "c").id in _ids(
        g.layer("_default").edges[Edge.is_deleted()]
    )

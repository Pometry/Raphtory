"""Write-path parity in depth: deletions, layers, batches, strict creates, errors.

``test_parity_writes`` establishes that a handful of writes read back the same
on both sides. This module goes after the parts of the write path where a
``RemoteGraph`` write has to be *re-encoded* as a GraphQL mutation and replayed
server-side, and where a mis-encoding still leaves a plausible-looking graph
behind:

* a **deletion** is a tombstone, not a removal — the edge stays in
  ``graph.edges`` and only ``deletions`` / ``is_deleted()`` move, so a dropped
  delete is invisible to any topology read;
* a **layer** argument is the easiest thing to lose in encoding — write it to
  the wrong layer (or to ``_default``) and the edge still exists, still has
  properties, and only the per-layer reads disagree;
* a **batch** write is the one call shape that has no local counterpart, so it
  is compared against the loop of single writes it is supposed to be equal to,
  down to the ``event_id`` each update receives;
* a **strict create** and the other misuse cases must *fail*, and fail without
  half-applying.

Two things every case here does that value-parity alone would not:

1. it asserts the write **moved** the state it claims to write, per side
   (``_assert_write_lands``) — a remote that silently discarded the mutation
   would otherwise agree with a local that did too, and pass;
2. where the write is meant to be rejected it asserts **both** sides rejected it
   *and* that neither was left changed (``_assert_write_rejected``) — every
   failure on either side surfaces as a bare ``Exception``, so the comparator's
   exception branch is nearly free and proves almost nothing by itself.

Writes mutate, so no case shares a graph pair: each builds its own.
"""

import pytest
from raphtory.graphql import RemoteEdgeAddition, RemoteNodeAddition, RemoteUpdate

from _parity import KNOWN_GAPS, assert_parity, canonical, graph_pair

# --- write-aware assertions -------------------------------------------------


def _sides(pair):
    return (("local", pair.local), ("remote", pair.remote))


def _snapshot(pair, probe):
    return {name: canonical(probe(g)) for name, g in _sides(pair)}


def _containment_facts(g):
    """Every layer's membership plus the unlayered totals.

    A caller's probe reads the place a write was *meant* to land, which proves
    it arrived but not that it arrived *only* there. This reads everywhere: the
    layer list, each layer's nodes/edges, and the graph-level totals. A write
    that also leaked into `_default`, or into a sibling layer, moves one of
    these even though the targeted probe looks perfect.

    Deliberately generic — no property names — so it can run after every write
    regardless of what that write touched.
    """
    layers = sorted(g.unique_layers)
    return {
        "layers": layers,
        "per_layer": {
            layer: (
                sorted(n.name for n in g.layer(layer).nodes),
                sorted((e.src.name, e.dst.name) for e in g.layer(layer).edges),
                g.layer(layer).count_edges(),
            )
            for layer in layers
        },
        "graph": (
            sorted(n.name for n in g.nodes),
            sorted((e.src.name, e.dst.name) for e in g.edges),
            g.count_nodes(),
            g.count_edges(),
        ),
    }


def _assert_write_lands(pair, write, probe):
    """Apply ``write`` to both graphs: the probe must move on *each* side, then match.

    The per-side movement check is the anti-vacuity guard. A write that was
    dropped on the floor leaves each side equal to its own ``before``, and the
    two sides still equal to each other — so a cross-side comparison on its own
    would call that parity.

    Then `_containment_facts` is compared across *all* layers and the whole
    graph, so a write that landed where it was asked to but *also* somewhere
    else is caught — the targeted probe alone cannot see that.
    """
    before = _snapshot(pair, probe)
    for _, g in _sides(pair):
        write(g)
    after = _snapshot(pair, probe)
    for name, _ in _sides(pair):
        assert after[name] != before[name], (
            f"{name}: the write left the probe at {before[name]!r} — it was a "
            f"no-op, which makes the parity assertion below vacuous"
        )
    assert_parity(pair, probe)
    assert_parity(pair, _containment_facts)
    return after


def _assert_write_rejected(pair, write, probe):
    """Both sides must reject ``write``, and neither may be changed by it.

    "Not changed" is asserted twice, at different strengths. The caller's probe
    is compared per side (the targeted read). Then `_containment_facts` — every
    layer plus the graph totals — is *also* compared per side against its own
    before-snapshot, not merely across sides: a rejected write has no oracle
    problem, nothing may change anywhere, so a stray layer conjured identically
    on both sides is still a failure here even though it would satisfy parity.
    """
    before = _snapshot(pair, probe)
    containment_before = _snapshot(pair, _containment_facts)
    for name, g in _sides(pair):
        try:
            value = write(g)
        except Exception:  # noqa: BLE001 — the point is only *that* it raised
            continue
        raise AssertionError(
            f"{name}: write was accepted (returned {value!r}), expected a failure"
        )
    after = _snapshot(pair, probe)
    containment_after = _snapshot(pair, _containment_facts)
    for name, _ in _sides(pair):
        assert after[name] == before[name], (
            f"{name}: a rejected write still changed state: "
            f"{before[name]!r} -> {after[name]!r}"
        )
        assert containment_after[name] == containment_before[name], (
            f"{name}: a rejected write still changed a layer or the totals: "
            f"{containment_before[name]!r} -> {containment_after[name]!r}"
        )
    assert_parity(pair, probe)


def _stamps(times):
    """``(timestamp, event_id)`` for a history/deletions sequence.

    The ``event_id`` is kept deliberately: it is the half of a write's identity
    that the default comparator reduction throws away, and it is what
    distinguishes two updates at the same timestamp.
    """
    return sorted((x.t, x.event_id) for x in times)


def _node_stamps(g, name):
    """``_stamps`` of a node's history, or ``None`` if the node does not exist.

    ``_assert_write_lands`` snapshots *before* the write, and the write is often
    what creates the node — so "not there yet" has to be a probe value rather
    than an ``AttributeError`` on ``None``.
    """
    node = g.node(name)
    return None if node is None else _stamps(node.history)


def _prop_timeline(props, key):
    """``(t, event_id, value)`` per update of a temporal property, ``()`` if unset.

    Same reason as ``_node_stamps``: answerable before the property exists. The
    ``event_id`` is kept, and the tuple is left unsorted, so the ordering the
    two sides report is compared rather than normalized away.
    """
    prop = props.temporal.get(key)
    return () if prop is None else tuple((t.t, t.event_id, v) for t, v in prop.items())


# --- 1. deletions -----------------------------------------------------------


def _build_deletable(g):
    """``a -> b`` in two named layers *and* the default one, plus a second edge.

    The default layer is seeded deliberately: ``Edge.delete()`` with no layer
    requires one to already exist (see
    ``test_edge_delete_without_a_default_layer_is_rejected_on_both_sides``),
    so without it half the call forms below could not be compared at all.
    Everything happens before t=6, which is when every delete form fires.
    """
    g.add_edge(1, "a", "b", layer="l1", properties={"w": 1.0})
    g.add_edge(2, "a", "b", layer="l2", properties={"w": 2.0})
    g.add_edge(3, "a", "b", properties={"w": 3.0})
    g.add_edge(4, "c", "d", layer="l1")


# name -> the delete call. Every call form that both sides accept: graph-level
# and edge-level, layer given and omitted, `event_id` given and omitted.
DELETE_FORMS = {
    "graph_positional": lambda g: g.delete_edge(6, "a", "b", "l1", None),
    "graph_positional_event_id": lambda g: g.delete_edge(6, "a", "b", "l1", 77),
    "graph_layer_kw": lambda g: g.delete_edge(6, "a", "b", layer="l1"),
    "graph_layer_and_event_id": lambda g: g.delete_edge(
        6, "a", "b", layer="l1", event_id=77
    ),
    "graph_no_layer": lambda g: g.delete_edge(6, "a", "b"),
    "graph_no_layer_event_id": lambda g: g.delete_edge(6, "a", "b", event_id=77),
    "edge_delete": lambda g: g.edge("a", "b").delete(6),
    "edge_delete_layer": lambda g: g.edge("a", "b").delete(6, layer="l1"),
    "edge_delete_layer_and_event_id": lambda g: g.edge("a", "b").delete(
        6, layer="l1", event_id=77
    ),
}


def _deletion_facts(g):
    """Everything a deletion is observable through, on the edge and the graph.

    Deliberately mixes reads that a tombstone *does* move (``deletions``,
    ``is_deleted``, ``is_valid``) with reads it must *not* move on an event
    graph (``graph.edges``, ``count_edges``) — so a delete implemented as a
    removal fails here just as loudly as one that was dropped.
    """
    edge = g.edge("a", "b")
    return (
        _stamps(edge.deletions),
        _stamps(edge.layer("_default").deletions),
        _stamps(edge.layer("l1").deletions),
        _stamps(edge.layer("l2").deletions),
        edge.is_deleted(),
        edge.is_valid(),
        edge.layer("l1").is_deleted(),
        edge.layer("l1").is_valid(),
        edge.latest_time,
        sorted(tuple(sorted(e.layer_names)) for e in g.edges),
        sorted((e.src.name, e.dst.name) for e in g.edges),
        g.count_edges(),
        # Windows either side of the tombstone: before it the edge is untouched,
        # over it the edge is deleted, and both sides must agree on which.
        sorted((e.src.name, e.dst.name, e.is_deleted()) for e in g.window(0, 5).edges),
        sorted((e.src.name, e.dst.name, e.is_deleted()) for e in g.window(6, 20).edges),
    )


@pytest.mark.parametrize("form", DELETE_FORMS.values(), ids=list(DELETE_FORMS))
def test_delete_edge_call_form_parity(form):
    """Every accepted ``delete_edge`` / ``Edge.delete`` form lands identically."""
    with graph_pair(_build_deletable) as pair:
        _assert_write_lands(pair, form, _deletion_facts)


@pytest.mark.parametrize("form", DELETE_FORMS.values(), ids=list(DELETE_FORMS))
def test_delete_edge_records_a_tombstone_on_both_sides(form):
    """The delete records a deletion and does *not* remove the edge, per side.

    Anchored per side rather than compared across, so "both sides removed the
    edge" and "neither side recorded anything" are both failures.
    """
    with graph_pair(_build_deletable) as pair:
        for name, g in _sides(pair):
            form(g)
            edge = g.edge("a", "b")
            assert edge is not None, f"{name}: delete removed the edge from the graph"
            assert _stamps(edge.deletions), f"{name}: no deletion was recorded"
            assert edge.is_deleted(), f"{name}: edge does not report as deleted"
            assert g.count_edges() == 2, (
                f"{name}: an event-graph delete changed count_edges to "
                f"{g.count_edges()} — it should only add a tombstone"
            )


def test_delete_edge_event_id_is_recorded_verbatim():
    """An explicit ``event_id`` is stored as given, not re-assigned by either side."""
    with graph_pair(_build_deletable) as pair:
        for name, g in _sides(pair):
            g.delete_edge(6, "a", "b", layer="l1", event_id=77)
            got = _stamps(g.edge("a", "b").layer("l1").deletions)
            assert got == [(6, 77)], f"{name}: expected [(6, 77)], got {got}"
        assert_parity(pair, lambda g: _stamps(g.edge("a", "b").layer("l1").deletions))


def test_delete_edge_only_tombstones_the_named_layer():
    """A layer-scoped delete leaves the other layer valid, on both sides."""
    with graph_pair(_build_deletable) as pair:
        for name, g in _sides(pair):
            g.delete_edge(6, "a", "b", layer="l1", event_id=77)
            edge = g.edge("a", "b")
            assert edge.layer("l1").is_deleted(), f"{name}: l1 not deleted"
            assert not edge.layer("l2").is_deleted(), (
                f"{name}: deleting l1 also tombstoned l2 — the layer argument "
                f"was not honoured"
            )
        assert_parity(
            pair,
            lambda g: [
                g.edge("a", "b").layer(layer).is_deleted() for layer in ("l1", "l2")
            ],
        )


def test_delete_edge_of_unknown_edge_creates_it_on_both_sides():
    """Deleting an edge that does not exist is *not* an error — it creates it.

    Recorded as a parity case rather than an error case because that is the
    behaviour both sides actually have: the delete implicitly creates the edge
    (and its endpoints) and immediately tombstones it. If either side ever
    starts rejecting it, this test fails rather than the misuse going unnoticed.
    """
    with graph_pair(_build_deletable) as pair:
        for name, g in _sides(pair):
            assert not g.has_edge("x", "y"), f"{name}: fixture already has x -> y"

        _assert_write_lands(
            pair,
            lambda g: g.delete_edge(8, "x", "y", None, None),
            lambda g: (
                g.has_edge("x", "y"),
                g.has_node("x"),
                g.has_node("y"),
                g.count_edges(),
                g.count_nodes(),
            ),
        )
        for name, g in _sides(pair):
            assert g.has_edge("x", "y"), f"{name}: delete did not create the edge"
            stamps = _stamps(g.edge("x", "y").deletions)
            assert (
                len(stamps) == 1 and stamps[0][0] == 8
            ), f"{name}: expected exactly one tombstone at t=8, got {stamps}"
        # The auto-assigned `event_id` on that tombstone must match too.
        assert_parity(pair, lambda g: _stamps(g.edge("x", "y").deletions))


def test_edge_delete_creates_the_default_layer_on_both_sides():
    """``edge.delete(t)`` with no layer creates the default layer if missing.

    The two spellings of "delete this edge, no layer given" must agree:
    ``graph.delete_edge(t, src, dst)`` has always created ``_default`` on
    demand, while ``edge.delete(t)`` used to refuse when the graph's layers
    were all named. That asymmetry is now fixed in the engine, so both sides
    accept it and record the same tombstone.
    """

    def build(g):
        g.add_edge(1, "a", "b", layer="l1")
        g.add_edge(2, "a", "b", layer="l2")

    def probe(g):
        return (_stamps(g.edge("a", "b").deletions), sorted(g.unique_layers))

    with graph_pair(build) as pair:
        _assert_write_lands(pair, lambda g: g.edge("a", "b").delete(6), probe)
        for name, g in _sides(pair):
            assert "_default" in g.unique_layers, (
                f"{name}: edge.delete() with no layer should have created the "
                f"default layer, layers are {sorted(g.unique_layers)}"
            )

    # And it is the *same* write as the graph-level call: applied to a fresh
    # pair, the two spellings leave identical state.
    with graph_pair(build) as pair:
        _assert_write_lands(pair, lambda g: g.delete_edge(6, "a", "b"), probe)


# --- 2. layer-scoped writes -------------------------------------------------

# (timestamp, src, dst, layer, weight). `a -> b` spans three layers including
# the implicit `_default`, so per-layer reads on a single edge must disagree
# with each other; `c -> d` sits in one layer only, so layer *membership* reads
# have something to drop.
LAYERED_EDGES = [
    (1, "a", "b", "l1", 1.0),
    (2, "a", "b", "l2", 2.0),
    (3, "a", "b", None, 3.0),
    (4, "b", "c", "l1", 4.0),
    (5, "c", "d", "l3", 5.0),
    (6, "a", "b", "l1", 6.0),
]

WRITTEN_LAYERS = ["_default", "l1", "l2", "l3"]


def _build_layered(g):
    for time, src, dst, layer, weight in LAYERED_EDGES:
        g.add_edge(time, src, dst, properties={"w": weight}, layer=layer)


def _layer_facts(g):
    return (
        sorted(g.edge("a", "b").layer_names),
        sorted(g.unique_layers),
        sorted(
            (e.layer_name, e.earliest_time, e.latest_time)
            for e in g.edge("a", "b").explode_layers()
        ),
        sorted((layer, g.has_layer(layer)) for layer in WRITTEN_LAYERS + ["nope"]),
        g.count_edges(),
    )


def _build_layer_seed(g):
    """One edge in one layer — enough for ``_layer_facts`` to be answerable."""
    g.add_edge(0, "a", "b", properties={"w": 0.0}, layer="seed")


def test_layer_scoped_writes_parity():
    with graph_pair(_build_layer_seed) as pair:
        _assert_write_lands(pair, _build_layered, _layer_facts)


def _per_layer_probe(layer):
    def probe(g):
        view = g.layer(layer)
        return (
            sorted((e.src.name, e.dst.name) for e in view.edges),
            view.count_edges(),
            sorted(_stamps(e.history) for e in view.edges),
            sorted(_prop_timeline(e.properties, "w") for e in view.edges),
        )

    return probe


def _layer_scoped_node_update(g):
    """A node update pinned to a named layer, which the server records against
    that layer rather than the default one."""
    g.node("a").add_updates(5, properties={"s": 1.0}, layer="seed")


def _node_update_layer_facts(g):
    # Probed before the write too, when the property does not exist yet.
    prop = g.node("a").properties.temporal.get("s")
    return (
        _prop_timeline(g.node("a").properties, "s"),
        None if prop is None else prop.at(5),
        None if prop is None else prop.at(4),
        sorted(g.unique_layers),
    )


def test_layer_scoped_node_update_parity():
    """``add_updates(..., layer=...)`` on a node writes the same thing on both
    sides — the layer argument reaches the server rather than being dropped."""
    with graph_pair(_build_layer_seed) as pair:
        _assert_write_lands(pair, _layer_scoped_node_update, _node_update_layer_facts)


@pytest.mark.parametrize("layer", WRITTEN_LAYERS)
def test_layer_view_after_layered_writes_parity(layer):
    """Reads under ``layer(name)`` agree, for every layer that was written."""
    with graph_pair(_build_layered) as pair:
        assert_parity(pair, _per_layer_probe(layer))


def test_each_written_layer_selects_a_different_edge_set():
    """The per-layer cases above are not all reading the same thing.

    Without this, a remote that ignored the ``layer`` argument on both the write
    and the read would answer every ``layer(...)`` case with the whole graph —
    and every per-layer parity case would pass.
    """
    with graph_pair(_build_layered) as pair:
        for name, g in _sides(pair):
            seen = []
            for layer in WRITTEN_LAYERS:
                facts = canonical(_per_layer_probe(layer)(g))
                assert facts not in seen, (
                    f"{name}: layer {layer!r} reads the same as an earlier layer — "
                    f"the layer argument was not honoured on the write or the read"
                )
                seen.append(facts)


@pytest.mark.parametrize(
    "names", [["l1", "l2"], ["l1", "l3"], ["_default", "l1", "l2", "l3"]], ids=str
)
def test_layers_view_after_layered_writes_parity(names):
    with graph_pair(_build_layered) as pair:
        assert_parity(
            pair,
            lambda g: (
                sorted((e.src.name, e.dst.name) for e in g.layers(names).edges),
                g.layers(names).count_edges(),
                sorted(tuple(sorted(e.layer_names)) for e in g.layers(names).edges),
            ),
        )


def test_layer_scoped_edge_metadata_parity():
    """``add_metadata(..., layer=)`` lands on that layer, and reads back layer-keyed."""

    def build(g):
        _build_layered(g)
        g.edge("a", "b").add_metadata({"m": "one"}, layer="l1")
        g.edge("a", "b").add_metadata({"m": "two"}, layer="l2")

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.edge("a", "b").layer("l1").metadata.get("m"))
        assert_parity(pair, lambda g: g.edge("a", "b").layer("l2").metadata.get("m"))
        # Unlayered, the same key reads back as a per-layer mapping.
        assert_parity(pair, lambda g: g.edge("a", "b").metadata.get("m"))
        for name, g in _sides(pair):
            assert g.edge("a", "b").layer("l1").metadata.get("m") == "one", name
            assert g.edge("a", "b").layer("l2").metadata.get("m") == "two", name


def test_node_write_can_create_a_layer():
    """``add_node(..., layer=)`` registers the layer on both sides."""
    with graph_pair(lambda g: None) as pair:
        _assert_write_lands(
            pair,
            lambda g: g.add_node(1, "a", properties={"q": 1.0}, layer="nl"),
            lambda g: (sorted(g.unique_layers), g.has_layer("nl")),
        )
        for name, g in _sides(pair):
            assert g.has_layer("nl"), f"{name}: node write did not create layer 'nl'"


# --- 3. batch writes --------------------------------------------------------

# (name, node_type, metadata, [(t, properties)]). "n1" appears twice so the
# batch has to merge two updates into one entity.
BATCH_NODES = [
    ("n1", "T1", {"m": 1}, [(10, {"s": 1.0}), (11, {"s": 2.0})]),
    ("n2", None, None, [(12, {"s": 3.0})]),
    ("n1", None, None, [(13, {"s": 4.0})]),
]

# (src, dst, layer, metadata, [(t, properties)]). "n1 -> n2" likewise appears
# twice, and once with no layer, so the default layer is exercised too.
BATCH_EDGES = [
    ("n1", "n2", "bl", {"em": 5}, [(14, {"w": 1.0})]),
    ("n1", "n2", "bl", None, [(15, {"w": 2.0})]),
    ("n2", "n1", None, None, [(16, {"w": 3.0})]),
]


def _build_batch(g):
    """Apply ``BATCH_NODES`` / ``BATCH_EDGES`` — batched on remote, looped locally.

    The local ``Graph`` has no ``add_nodes`` / ``add_edges`` (ledgered as
    ``graph.add_nodes`` / ``graph.add_edges``), so the local side replays the
    same updates as individual ``add_node`` / ``add_edge`` calls **in the same
    order**. Order matters: it is what fixes the auto-assigned ``event_id`` of
    each update, and the probes below compare those ids — which is the whole
    point of the comparison. The batch call is only correct if it is equal to
    the loop it stands in for.
    """
    if hasattr(g, "add_nodes"):
        g.add_nodes(
            [
                RemoteNodeAddition(
                    name,
                    node_type=node_type,
                    metadata=metadata,
                    updates=[RemoteUpdate(t, props) for t, props in updates],
                )
                for name, node_type, metadata, updates in BATCH_NODES
            ]
        )
        g.add_edges(
            [
                RemoteEdgeAddition(
                    src,
                    dst,
                    layer=layer,
                    metadata=metadata,
                    updates=[RemoteUpdate(t, props) for t, props in updates],
                )
                for src, dst, layer, metadata, updates in BATCH_EDGES
            ]
        )
        return

    for name, node_type, metadata, updates in BATCH_NODES:
        for t, props in updates:
            g.add_node(t, name, properties=props, node_type=node_type)
        if metadata:
            g.node(name).add_metadata(metadata)
    for src, dst, layer, metadata, updates in BATCH_EDGES:
        for t, props in updates:
            g.add_edge(t, src, dst, properties=props, layer=layer)
        if metadata:
            g.edge(src, dst).add_metadata(metadata, layer=layer)


def _batch_state(g):
    return (
        sorted((n.name, n.node_type, _stamps(n.history)) for n in g.nodes),
        sorted(
            (n.name, _prop_timeline(n.properties, "s"), n.metadata.get("m"))
            for n in g.nodes
        ),
        sorted(
            (
                e.src.name,
                e.dst.name,
                tuple(sorted(e.layer_names)),
                _stamps(e.history),
                _prop_timeline(e.properties, "w"),
            )
            for e in g.edges
        ),
        sorted((e.src.name, e.dst.name, str(e.metadata.get("em"))) for e in g.edges),
        sorted(g.unique_layers),
        g.count_nodes(),
        g.count_edges(),
        g.count_temporal_edges(),
        g.earliest_time,
        g.latest_time,
    )


def test_batch_writes_match_the_equivalent_loop():
    """A remote batch is indistinguishable from the local loop of single writes."""
    with graph_pair(lambda g: None) as pair:
        _assert_write_lands(pair, _build_batch, _batch_state)


def test_batch_write_merges_repeated_entities():
    """Two entries for the same entity merge, rather than the last one winning."""
    with graph_pair(_build_batch) as pair:
        for name, g in _sides(pair):
            times = [t.t for t, _ in g.node("n1").properties.temporal.get("s").items()]
            assert times == [
                10,
                11,
                13,
            ], f"{name}: n1 kept {times} — the repeated batch entry did not merge"
            assert g.count_nodes() == 2, f"{name}: got {g.count_nodes()} nodes"
            weights = [
                t.t for t, _ in g.edge("n1", "n2").properties.temporal.get("w").items()
            ]
            assert weights == [14, 15], (
                f"{name}: n1 -> n2 kept {weights} — the repeated batch entry did "
                f"not merge"
            )
        assert_parity(pair, _batch_state)


def test_batch_write_keeps_node_type_and_metadata():
    with graph_pair(_build_batch) as pair:
        assert_parity(pair, lambda g: g.node("n1").node_type)
        assert_parity(pair, lambda g: g.node("n1").metadata.get("m"))
        assert_parity(pair, lambda g: str(g.edge("n1", "n2").metadata.get("em")))
        for name, g in _sides(pair):
            assert g.node("n1").node_type == "T1", name
            assert g.node("n1").metadata.get("m") == 1, name


# --- 4. create_node (strict create) -----------------------------------------


def _build_for_create(g):
    g.add_node(1, "existing", node_type="T", properties={"x": 1})
    g.add_edge(2, "existing", "other")


def _create_probe(g):
    return (
        sorted(n.name for n in g.nodes),
        g.count_nodes(),
        sorted((n.name, n.node_type) for n in g.nodes),
        _node_stamps(g, "existing"),
        _prop_timeline(g.node("existing").properties, "x"),
        _node_stamps(g, "fresh"),
    )


def test_create_node_success_parity():
    with graph_pair(_build_for_create) as pair:
        _assert_write_lands(
            pair,
            lambda g: g.create_node(
                5, "fresh", properties={"score": 2.0}, node_type="NT"
            ),
            _create_probe,
        )
        assert_parity(pair, lambda g: g.node("fresh").node_type)
        assert_parity(pair, lambda g: g.node("fresh").properties.get("score"))
        assert_parity(pair, lambda g: _stamps(g.node("fresh").history))
        for name, g in _sides(pair):
            assert g.node("fresh").node_type == "NT", name
            assert g.node("fresh").properties.get("score") == 2.0, name


def test_create_node_duplicate_is_rejected_on_both_sides():
    """The strict-create contract: the second create fails and changes nothing.

    ``_assert_write_rejected`` carries the weight here. Both sides raise a bare
    ``Exception``, so exception-type parity is satisfied by *any* failure; what
    makes this case real is that ``_create_probe`` — node set, node types, and
    the target's own history and property timeline — is unchanged afterwards on
    each side.
    """
    with graph_pair(_build_for_create) as pair:
        _assert_write_rejected(
            pair, lambda g: g.create_node(5, "existing"), _create_probe
        )


def test_create_node_duplicate_of_a_created_node_is_rejected():
    """Strict-create also refuses to re-create a node *it* created."""

    def build(g):
        _build_for_create(g)
        g.create_node(5, "fresh", node_type="NT")

    with graph_pair(build) as pair:
        _assert_write_rejected(
            pair,
            lambda g: g.create_node(6, "fresh", node_type="NT"),
            lambda g: (
                sorted(n.name for n in g.nodes),
                _stamps(g.node("fresh").history),
                g.node("fresh").node_type,
            ),
        )


def test_add_node_after_create_node_still_updates():
    """A rejected create must not have poisoned the node: ``add_node`` still works."""

    def build(g):
        _build_for_create(g)
        g.create_node(5, "fresh", node_type="NT")

    with graph_pair(build) as pair:
        for _, g in _sides(pair):
            with pytest.raises(Exception):
                g.create_node(6, "fresh")
        _assert_write_lands(
            pair,
            lambda g: g.add_node(7, "fresh", properties={"score": 9.0}),
            lambda g: (
                _stamps(g.node("fresh").history),
                g.node("fresh").properties.get("score"),
                g.node("fresh").latest_time,
            ),
        )


def test_event_id_counter_survives_a_rejected_write_identically():
    """A rejected write must not advance the auto ``event_id`` on one side only.

    Server-side replay makes this a genuine risk: a remote mutation can consume
    a sequence number before it fails validation. If it did, the very next
    successful write would be stamped with a different ``event_id`` than the
    local one, and every later ``(t, event_id)`` comparison would drift.
    """
    with graph_pair(_build_for_create) as pair:
        for _, g in _sides(pair):
            with pytest.raises(Exception):
                g.create_node(5, "existing")
            with pytest.raises(Exception):
                g.add_node(6, "existing", properties={"x": "wrong type"})
        _assert_write_lands(
            pair,
            lambda g: g.add_node(7, "after_failures", properties={"y": 1.0}),
            lambda g: _node_stamps(g, "after_failures"),
        )


# --- 5. node_type and metadata ----------------------------------------------


def test_set_node_type_parity():
    with graph_pair(lambda g: g.add_node(1, "a")) as pair:
        _assert_write_lands(
            pair,
            lambda g: g.node("a").set_node_type("person"),
            lambda g: (g.node("a").node_type, sorted(g.get_all_node_types())),
        )
        for name, g in _sides(pair):
            assert g.node("a").node_type == "person", name


def test_set_node_type_to_the_same_type_is_accepted():
    """Idempotent re-set: accepted on both sides, and the type is untouched."""

    def build(g):
        g.add_node(1, "a")
        g.node("a").set_node_type("person")

    with graph_pair(build) as pair:
        for name, g in _sides(pair):
            g.node("a").set_node_type("person")
            assert g.node("a").node_type == "person", name
        assert_parity(pair, lambda g: g.node("a").node_type)


def test_set_node_type_to_a_different_type_is_rejected_on_both_sides():
    def build(g):
        g.add_node(1, "a")
        g.node("a").set_node_type("person")

    with graph_pair(build) as pair:
        _assert_write_rejected(
            pair,
            lambda g: g.node("a").set_node_type("robot"),
            lambda g: (g.node("a").node_type, sorted(g.get_all_node_types())),
        )
        for name, g in _sides(pair):
            assert (
                g.node("a").node_type == "person"
            ), f"{name}: the rejected set_node_type still changed the type"


# how to reach each metadata-carrying handle, and how to read its metadata back
METADATA_TARGETS = {
    "graph": (lambda g: g, lambda g: g.metadata),
    "node": (lambda g: g.node("a"), lambda g: g.node("a").metadata),
    "edge": (lambda g: g.edge("a", "b"), lambda g: g.edge("a", "b").metadata),
}


def _build_metadata_base(g):
    g.add_node(1, "a")
    g.add_edge(2, "a", "b")


def _metadata_probe(read):
    def probe(g):
        meta = read(g)
        return (
            sorted(meta.keys()),
            {k: str(v) for k, v in meta.items()},
            str(meta.get("k")),
            len(meta),
            "k" in meta,
        )

    return probe


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_add_metadata_parity(target):
    reach, read = target
    with graph_pair(_build_metadata_base) as pair:
        _assert_write_lands(
            pair,
            lambda g: reach(g).add_metadata({"k": 1, "other": "x"}),
            _metadata_probe(read),
        )
        for name, g in _sides(pair):
            assert read(g).get("k") == 1, name


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_update_metadata_overwrites_parity(target):
    reach, read = target

    def build(g):
        _build_metadata_base(g)
        reach(g).add_metadata({"k": 1})

    with graph_pair(build) as pair:
        _assert_write_lands(
            pair, lambda g: reach(g).update_metadata({"k": 2}), _metadata_probe(read)
        )
        for name, g in _sides(pair):
            assert read(g).get("k") == 2, f"{name}: update_metadata did not overwrite"


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_add_metadata_conflicting_value_is_rejected_on_both_sides(target):
    """``add_metadata`` is write-once: a *different* value for a set key fails."""
    reach, read = target

    def build(g):
        _build_metadata_base(g)
        reach(g).add_metadata({"k": 1})

    with graph_pair(build) as pair:
        _assert_write_rejected(
            pair, lambda g: reach(g).add_metadata({"k": 2}), _metadata_probe(read)
        )
        for name, g in _sides(pair):
            assert (
                read(g).get("k") == 1
            ), f"{name}: the rejected add_metadata still changed the value"


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_add_metadata_identical_value_is_accepted(target):
    """Re-adding the *same* value is a no-op, not a conflict — on both sides."""
    reach, read = target

    def build(g):
        _build_metadata_base(g)
        reach(g).add_metadata({"k": 1})

    with graph_pair(build) as pair:
        # Pin the seeded value first: an idempotent write is the one case where
        # "unchanged afterwards" is also what a *dropped* write looks like, so
        # the before-state has to be established rather than assumed.
        for name, g in _sides(pair):
            assert read(g).get("k") == 1, f"{name}: the seed write did not land"
        for name, g in _sides(pair):
            reach(g).add_metadata({"k": 1})
            assert read(g).get("k") == 1, name
        assert_parity(pair, _metadata_probe(read))


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_update_metadata_type_conflict_is_rejected_on_both_sides(target):
    """``update_metadata`` may overwrite a value but not change its dtype."""
    reach, read = target

    def build(g):
        _build_metadata_base(g)
        reach(g).add_metadata({"k": 1})

    with graph_pair(build) as pair:
        _assert_write_rejected(
            pair,
            lambda g: reach(g).update_metadata({"k": "not an int"}),
            _metadata_probe(read),
        )


@pytest.mark.parametrize(
    "target", METADATA_TARGETS.values(), ids=list(METADATA_TARGETS)
)
def test_metadata_has_no_get_dtype_of_on_either_side(target):
    """``get_dtype_of`` is a ``Properties`` method, not a ``Metadata`` one — on both.

    Pinned because the *absence* is the parity claim: if one side grows the
    method the other lacks, a metadata dtype comparison would start passing
    locally and failing remotely (or vice versa) with no test to name it.
    """
    _, read = target
    with graph_pair(_build_metadata_base) as pair:
        assert_parity(pair, lambda g: hasattr(read(g), "get_dtype_of"))
        for name, g in _sides(pair):
            assert not hasattr(read(g), "get_dtype_of"), (
                f"{name}: Metadata grew get_dtype_of — update this test and add "
                f"a dtype parity case"
            )


@pytest.mark.parametrize(
    "target",
    [
        (lambda g: g.node("a"), lambda g: g.node("a").properties),
        (lambda g: g.edge("a", "b"), lambda g: g.edge("a", "b").properties),
    ],
    ids=["node", "edge"],
)
def test_written_property_dtype_readback_parity(target):
    """A temporal property written through either side reports the same dtype."""
    reach, read = target
    with graph_pair(_build_metadata_base) as pair:
        _assert_write_lands(
            pair,
            lambda g: reach(g).add_updates(5, properties={"count": 3, "ratio": 1.5}),
            lambda g: sorted(
                (key, repr(read(g).get_dtype_of(key))) for key in ("count", "ratio")
            ),
        )
        for name, g in _sides(pair):
            assert repr(read(g).get_dtype_of("count")) == "PropType.I64", name
            assert repr(read(g).get_dtype_of("ratio")) == "PropType.F64", name


# --- 6. property updates over time ------------------------------------------

# Repeated updates on one entity: three distinct timestamps, plus two writes at
# the *same* timestamp separated only by `event_id`, plus a same-timestamp write
# with no explicit id at all.
TIMELINE = [
    (5, 100, 1.0),
    (7, 101, 2.0),
    (7, 102, 3.0),
    (9, None, 4.0),
]


def _seed_node(g):
    g.add_node(1, "a")


def _seed_edge(g):
    g.add_edge(1, "a", "b")


def _write_node_timeline(g):
    for t, event_id, value in TIMELINE:
        g.node("a").add_updates(t, properties={"s": value}, event_id=event_id)


def _write_edge_timeline(g):
    for t, event_id, value in TIMELINE:
        g.edge("a", "b").add_updates(t, properties={"s": value}, event_id=event_id)


# kind -> (seed the entity, apply the timeline, reach the entity). Seed and
# timeline are separate so `_assert_write_lands` can snapshot a graph in which
# the entity exists but the timeline has not been written yet.
TIMELINE_KINDS = {
    "node": (_seed_node, _write_node_timeline, lambda g: g.node("a")),
    "edge": (_seed_edge, _write_edge_timeline, lambda g: g.edge("a", "b")),
}


def _build_timeline(kind):
    seed, write, _ = TIMELINE_KINDS[kind]

    def build(g):
        seed(g)
        write(g)

    return build


def _timeline_probe(reach):
    def probe(g):
        handle = reach(g)
        prop = handle.properties.temporal.get("s")
        return (
            _prop_timeline(handle.properties, "s"),
            None if prop is None else prop.count(),
            None if prop is None else prop.value(),
            None if prop is None else sorted(prop.unique()),
            _stamps(handle.history),
            handle.earliest_time,
            handle.latest_time,
        )

    return probe


@pytest.mark.parametrize("kind", list(TIMELINE_KINDS))
def test_repeated_add_updates_timeline_parity(kind):
    """A timeline of repeated updates, including two at the same timestamp."""
    seed, write, reach = TIMELINE_KINDS[kind]
    with graph_pair(seed) as pair:
        _assert_write_lands(pair, write, _timeline_probe(reach))


@pytest.mark.parametrize("kind", list(TIMELINE_KINDS))
def test_same_timestamp_updates_are_both_retained(kind):
    """Two updates at one timestamp survive as two entries, ordered by ``event_id``.

    This is what the ``event_id`` is *for*, and the check that a same-timestamp
    write is not treated as an overwrite. Asserted per side against the exact
    expected timeline, so "both sides collapsed them into one" fails.
    """
    _, _, reach = TIMELINE_KINDS[kind]
    with graph_pair(_build_timeline(kind)) as pair:
        for name, g in _sides(pair):
            items = [
                (t.t, t.event_id, v)
                for t, v in reach(g).properties.temporal.get("s").items()
            ]
            at_seven = [entry for entry in items if entry[0] == 7]
            assert len(at_seven) == 2, (
                f"{name}: t=7 kept {at_seven} — the two same-timestamp updates "
                f"were collapsed"
            )
            assert [entry[1] for entry in at_seven] == [101, 102], (
                f"{name}: same-timestamp updates are not ordered by event_id: "
                f"{at_seven}"
            )
            assert [entry[2] for entry in at_seven] == [
                2.0,
                3.0,
            ], f"{name}: same-timestamp values are wrong: {at_seven}"
        assert_parity(pair, _timeline_probe(reach))


@pytest.mark.parametrize("kind", list(TIMELINE_KINDS))
@pytest.mark.parametrize("time", [1, 5, 6, 7, 9, 20])
def test_timeline_at_reads_parity(kind, time):
    """``at(t)`` over the written timeline agrees at every interesting instant."""
    _, _, reach = TIMELINE_KINDS[kind]
    with graph_pair(_build_timeline(kind)) as pair:
        assert_parity(pair, lambda g: reach(g).properties.temporal.get("s").at(time))
        assert_parity(pair, lambda g: reach(g).at(time).properties.get("s"))
        assert_parity(
            pair,
            lambda g: (
                reach(g).at(time).earliest_time,
                reach(g).at(time).latest_time,
                _stamps(reach(g).at(time).history),
            ),
        )


@pytest.mark.parametrize("kind", list(TIMELINE_KINDS))
def test_timeline_at_reads_are_not_all_equal(kind):
    """The ``at(t)`` cases above read different values — otherwise they are vacuous."""
    _, _, reach = TIMELINE_KINDS[kind]
    with graph_pair(_build_timeline(kind)) as pair:
        for name, g in _sides(pair):
            values = [reach(g).properties.temporal.get("s").at(t) for t in (1, 5, 7, 9)]
            assert len(set(map(repr, values))) == 4, (
                f"{name}: at() returned {values} across four instants — the "
                f"timeline is not being sliced"
            )


@pytest.mark.parametrize("kind", list(TIMELINE_KINDS))
def test_timeline_latest_read_parity(kind):
    _, _, reach = TIMELINE_KINDS[kind]
    with graph_pair(_build_timeline(kind)) as pair:
        assert_parity(pair, lambda g: reach(g).properties.temporal.get("s").value())
        assert_parity(pair, lambda g: reach(g).properties.get("s"))
        assert_parity(pair, lambda g: reach(g).latest().latest_time)
        for name, g in _sides(pair):
            assert reach(g).properties.get("s") == 4.0, (
                f"{name}: latest value is {reach(g).properties.get('s')!r}, "
                f"expected the t=9 write"
            )


def test_graph_add_properties_timeline_parity():
    """Graph-level temporal properties, including a same-timestamp pair."""

    def write(g):
        g.add_properties(5, {"gp": 1.0}, event_id=10)
        g.add_properties(5, {"gp": 2.0}, event_id=11)
        g.add_properties(6, {"gp": 3.0})

    with graph_pair(lambda g: g.add_node(1, "a")) as pair:
        _assert_write_lands(
            pair,
            write,
            lambda g: (
                _prop_timeline(g.properties, "gp"),
                sorted(g.properties.temporal.keys()),
                g.properties.get("gp"),
            ),
        )
        for name, g in _sides(pair):
            items = _prop_timeline(g.properties, "gp")
            # The two explicit ids are pinned; the third write's auto-assigned id
            # is left to the implementation and only compared across sides by
            # `_assert_write_lands` above.
            assert items[:2] == (
                (5, 10, 1.0),
                (5, 11, 2.0),
            ), f"{name}: graph property timeline is {items}"
            assert (items[2][0], items[2][2]) == (
                6,
                3.0,
            ), f"{name}: graph property timeline is {items}"


# --- 7. error-path parity ---------------------------------------------------


def _build_typed_base(g):
    g.add_node(1, "a", properties={"x": 1})
    g.add_edge(2, "a", "b", properties={"w": 1.0})


def _typed_probe(g):
    return (
        _stamps(g.node("a").history),
        tuple(
            (t.t, t.event_id, v)
            for t, v in g.node("a").properties.temporal.get("x").items()
        ),
        repr(g.node("a").properties.get_dtype_of("x")),
        _stamps(g.edge("a", "b").history),
        tuple(
            (t.t, t.event_id, v)
            for t, v in g.edge("a", "b").properties.temporal.get("w").items()
        ),
        repr(g.edge("a", "b").properties.get_dtype_of("w")),
        g.count_nodes(),
        g.count_edges(),
    )


# name -> a write that conflicts with a dtype already established by
# `_build_typed_base`. Each must be rejected, and reject *cleanly*.
TYPE_CONFLICTS = {
    "add_node_property": lambda g: g.add_node(5, "a", properties={"x": "str"}),
    "node_add_updates": lambda g: g.node("a").add_updates(5, properties={"x": "str"}),
    "add_edge_property": lambda g: g.add_edge(5, "a", "b", properties={"w": "str"}),
    "edge_add_updates": lambda g: g.edge("a", "b").add_updates(
        5, properties={"w": "str"}
    ),
    "create_node_property": lambda g: g.create_node(5, "b", properties={"x": "str"}),
}


@pytest.mark.parametrize("write", TYPE_CONFLICTS.values(), ids=list(TYPE_CONFLICTS))
def test_property_type_conflict_is_rejected_on_both_sides(write):
    """Writing a property whose dtype conflicts with the first write fails cleanly.

    The probe pins the dtype as well as the values: a side that *widened* the
    column to accept the new value instead of rejecting it would keep the
    timeline plausible and only the dtype would move.
    """
    with graph_pair(_build_typed_base) as pair:
        _assert_write_rejected(pair, write, _typed_probe)


@pytest.mark.parametrize("write", TYPE_CONFLICTS.values(), ids=list(TYPE_CONFLICTS))
def test_a_well_typed_write_after_a_conflict_still_lands(write):
    """The rejection is not fatal: the same key still accepts a well-typed value.

    Guards the other failure mode of ``_assert_write_rejected`` — a side that
    rejected the write by wedging the entity would pass "state unchanged" and
    still be broken.
    """
    with graph_pair(_build_typed_base) as pair:
        for _, g in _sides(pair):
            with pytest.raises(Exception):
                write(g)
        _assert_write_lands(
            pair,
            lambda g: g.node("a").add_updates(6, properties={"x": 42}),
            lambda g: tuple(
                (t.t, t.event_id, v)
                for t, v in g.node("a").properties.temporal.get("x").items()
            ),
        )


def test_add_edge_with_unknown_endpoints_creates_them_on_both_sides():
    """Missing endpoints are *not* an error — both sides create them implicitly.

    Recorded as behaviour parity rather than an error case, so that if either
    side ever starts rejecting an edge into an unknown node the divergence is
    named here instead of surfacing as a mysterious count mismatch.
    """
    with graph_pair(_build_typed_base) as pair:
        _assert_write_lands(
            pair,
            lambda g: g.add_edge(9, "unknown_src", "unknown_dst"),
            lambda g: (
                sorted(n.name for n in g.nodes),
                g.count_nodes(),
                g.count_edges(),
                g.has_node("unknown_src"),
                g.has_node("unknown_dst"),
            ),
        )
        for name, g in _sides(pair):
            assert g.has_node("unknown_src") and g.has_node(
                "unknown_dst"
            ), f"{name}: endpoints were not implicitly created"


def test_node_type_conflict_leaves_the_rest_of_the_write_intact():
    """A ``node_type`` conflict on ``add_node`` rejects, and nothing else lands."""

    def build(g):
        g.add_node(1, "a", node_type="person", properties={"x": 1})

    with graph_pair(build) as pair:
        _assert_write_rejected(
            pair,
            lambda g: g.add_node(5, "a", properties={"x": 2}, node_type="robot"),
            lambda g: (
                g.node("a").node_type,
                _stamps(g.node("a").history),
                tuple(
                    (t.t, t.event_id, v)
                    for t, v in g.node("a").properties.temporal.get("x").items()
                ),
            ),
        )


# --- ledgered write-path gaps -----------------------------------------------

# (gap_key, fn). Same contract as `test_parity_gaps`: the case is expected to
# fail today because the API is missing on one side, and `strict=True` turns the
# day it starts working into a RED suite that forces the ledger entry out.
# Unlike that module, these gaps run in *both* directions — `graph.add_nodes`
# and `graph.add_edges` exist on remote and are missing locally.
WRITE_GAP_CASES = [
    (
        "graph.add_nodes",
        lambda g: g.add_nodes(
            [RemoteNodeAddition("z", updates=[RemoteUpdate(1, {"s": 1.0})])]
        ),
    ),
    (
        "graph.add_edges",
        lambda g: g.add_edges(
            [RemoteEdgeAddition("a", "b", updates=[RemoteUpdate(1, {"w": 1.0})])]
        ),
    ),
]


@pytest.mark.parametrize(
    "key,fn",
    [
        pytest.param(
            key,
            fn,
            marks=pytest.mark.xfail(reason=KNOWN_GAPS[key], strict=True),
            id=key,
        )
        for key, fn in WRITE_GAP_CASES
    ],
)
def test_known_write_gap(key, fn):
    def build(g):
        g.add_node(1, "a", properties={"s": 0.0})
        g.add_edge(2, "a", "b")

    with graph_pair(build) as pair:
        assert_parity(pair, fn)


def test_write_gap_cases_are_all_ledgered():
    for key, _ in WRITE_GAP_CASES:
        assert key in KNOWN_GAPS, f"gap case {key!r} missing from KNOWN_GAPS ledger"


# --- delete-edge argument parity ---------------------------------------------


def test_delete_edge_takes_an_event_id_on_both_sides():
    """`delete_edge` accepts the same arguments — ``event_id`` included — on both.

    The signatures used to diverge (remote had no ``event_id`` at all), so a
    graph-agnostic build could not call it. One call form now works on both, and
    the tombstone it records — timestamp *and* event id — must match. Built on
    its own pair rather than the module fixture, because it writes.
    """

    def build(g):
        g.add_node(1, "a")
        g.add_node(1, "b")
        g.add_edge(2, "a", "b", layer="knows")
        g.delete_edge(5, "a", "b", layer="knows", event_id=7)

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.edge("a", "b").is_deleted())
        assert_parity(
            pair,
            lambda g: sorted(
                (t.t, t.event_id) for t in g.edge("a", "b").layer("knows").deletions
            ),
        )
        assert_parity(pair, lambda g: g.count_edges())

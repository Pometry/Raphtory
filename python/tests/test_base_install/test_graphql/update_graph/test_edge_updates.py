import tempfile
from datetime import datetime, timezone
import pytest
from utils import assert_has_metadata, assert_has_properties
from raphtory.graphql import GraphServer, RaphtoryClient
from numpy.testing import assert_equal as check_arr


def make_props():
    current_datetime = datetime.now(timezone.utc)
    naive_datetime = datetime.now()
    return {
        "prop_string": "blah",
        "prop_float": 2.0,
        "prop_int": 2,
        "prop_bool": True,
        "prop_map": {
            "prop_inner_string": "blah",
            "prop_inner_float": 2.0,
            "prop_inner_int": 2,
            "prop_inner_bool": True,
            "prop_inner_map": {
                "prop_inner_inner_string": "hhhh",
                "prop_inner_inner_float": 10.0,
                "prop_inner_inner_int": 5,
                "prop_inner_inner_bool": False,
            },
        },
        "prop_array": [1, 2, 3, 4, 5, 6],
        "prop_datetime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def make_props2():
    current_datetime = datetime.now(timezone.utc)
    naive_datetime = datetime.now()
    return {
        "prop_string": "blah2",
        "prop_float": 3.0,
        "prop_int": 23,
        "prop_bool": False,
        "prop_map": {
            "prop_inner_string": "b",
            "prop_inner_float": 6.0,
            "prop_inner_int": 332,
            "prop_inner_bool": True,
        },
        "prop_array": [1, 2, 3, 5, 6],
        "prop_datetime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def test_add_updates():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        edge = rg.add_edge(1, "ben", "hamza")
        edge.add_updates(2, props, layer="test")
        edge.add_updates(3, props)
        edge.add_updates(4, layer="test2")
        edge.add_updates(5)
        rg.edge("ben", "hamza").add_updates(6)
        g = client.receive_graph("path/to/event_graph")
        e = g.edge("ben", "hamza")
        assert_has_properties(e, props)
        check_arr(e.properties.temporal.get("prop_float").history.t.collect(), [2, 3])
        check_arr(e.layer("test").properties.temporal.get("prop_float").history.t.collect(), [2])
        check_arr(e.history.t.collect(), [1, 2, 3, 4, 5, 6])


def test_add_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        props2 = make_props2()
        edge = rg.add_edge(1, "ben", "hamza")
        rg.add_edge(1, "ben", "hamza", layer="test")

        edge.add_metadata(props)
        rg.edge("ben", "hamza").add_metadata(props2, layer="test")
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g.edge("ben", "hamza").layer("_default"), props)
        assert_has_metadata(g.edge("ben", "hamza").layer("test"), props2)

        with pytest.raises(Exception) as excinfo:
            rg.edge("ben", "hamza").add_metadata({"prop_float": 3.0})
        assert "Attempted to change value of metadata" in str(excinfo.value)


def test_update_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        props2 = make_props2()
        edge = rg.add_edge(1, "ben", "hamza")
        rg.add_edge(1, "ben", "hamza", layer="test")

        edge.update_metadata(props)
        rg.edge("ben", "hamza").update_metadata(props2, layer="test")
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g.edge("ben", "hamza").layer("_default"), props)
        assert_has_metadata(g.edge("ben", "hamza").layer("test"), props2)

        edge.update_metadata(props2)
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g.edge("ben", "hamza").layer("_default"), props2)


def test_delete():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")

        edge = rg.add_edge(1, "ben", "hamza")
        edge.delete(2)
        g = client.receive_graph("path/to/event_graph")
        assert g.edge("ben", "hamza").deletions.t == [2]

        client.new_graph("path/to/persistent_graph", "PERSISTENT")
        rg = client.remote_graph("path/to/persistent_graph")
        rg.add_edge(1, "ben", "hamza")
        rg.edge("ben", "hamza").delete(2)
        edge = rg.add_edge(1, "ben", "lucas", layer="colleagues")
        edge.delete(2, layer="colleagues")
        g = client.receive_graph("path/to/persistent_graph")
        assert g.edge("ben", "hamza").deletions == [(2, 1)]
        assert g.edge("ben", "lucas").deletions == [(2, 3)]

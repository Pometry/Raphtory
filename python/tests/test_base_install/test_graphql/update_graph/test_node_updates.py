import tempfile
from datetime import datetime, timezone
import pytest
from dateutil import parser

from utils import assert_has_properties, assert_has_metadata
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
            "prop_map": {
                "prop_inner_string": "b",
                "prop_inner_float": 6.0,
                "prop_inner_int": 332,
                "prop_inner_bool": True,
            },
            "prop_inner_float": 2.0,
            "prop_inner_int": 2,
            "prop_inner_bool": True,
        },
        "prop_array": [1, 2, 3, 4, 5, 6],
        "prop_datetime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def test_set_node_type():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        node = rg.add_node(1, "ben")
        node.set_node_type("person")
        rg.add_node(2, "hamza")
        rg.node("hamza").set_node_type("person")
        g = client.receive_graph("path/to/event_graph")
        assert g.node("ben").node_type == "person"
        assert g.node("hamza").node_type == "person"


def test_add_updates():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        node = rg.add_node(1, "ben")
        node.add_updates(2, props)
        node.add_updates(3)
        rg.node("ben").add_updates(4)
        g = client.receive_graph("path/to/event_graph")
        assert_has_properties(g.node("ben"), props)
        check_arr(g.node("ben").history(), [1, 2, 3, 4])


def test_add_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        node = rg.add_node(1, "ben")
        node.add_metadata(props)
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g.node("ben"), props)

        with pytest.raises(Exception) as excinfo:
            rg.node("ben").add_metadata({"prop_float": 3.0})
        assert "Attempted to change value of constant property" in str(excinfo.value)


def test_update_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        node = rg.add_node(1, "ben")
        node.update_metadata(props)
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g.node("ben"), props)

        rg.node("ben").update_metadata({"prop_float": 3.0})
        g = client.receive_graph("path/to/event_graph")
        assert g.node("ben").metadata.get("prop_float") == 3.0

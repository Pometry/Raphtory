import tempfile
import pytest
from dateutil import parser
from raphtory.graphql import GraphServer, RaphtoryClient
from datetime import datetime, timezone
from numpy.testing import assert_equal as check_arr
from utils import assert_set_eq, assert_has_metadata, assert_has_properties


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


def test_add_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        rg.add_metadata(props)
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g, props)

        with pytest.raises(Exception) as excinfo:
            rg.add_metadata({"prop_float": 3.0})
        assert "Attempted to change value of metadata" in str(excinfo.value)


def test_update_metadata():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()

        rg.update_metadata(props)
        g = client.receive_graph("path/to/event_graph")
        assert_has_metadata(g, props)

        rg.update_metadata({"prop_float": 3.0})
        g = client.receive_graph("path/to/event_graph")
        assert g.metadata.get("prop_float") == 3.0


def test_add_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        rg.add_property(1, props)
        current_datetime = datetime.now(timezone.utc)
        naive_datetime = datetime.now()
        rg.add_property(current_datetime, props)
        rg.add_property(naive_datetime, props)
        g = client.receive_graph("path/to/event_graph")
        assert_has_properties(g, props)

        localized_datetime = naive_datetime.replace(tzinfo=timezone.utc)
        timestamps = [
            1,
            int(current_datetime.timestamp() * 1000),
            int(localized_datetime.timestamp() * 1000),
        ]

        check_arr(g.properties.temporal.get("prop_map").history(), timestamps)


def test_add_node():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        current_datetime = datetime.now(timezone.utc)
        rg.add_node(current_datetime, "ben", properties=props, node_type="person")
        rg.add_node(current_datetime, 1)  # This gets stringified on the server
        rg.add_node(current_datetime, "hamza", node_type="person")
        g = client.receive_graph("path/to/event_graph")
        assert g.node("ben").node_type == "person"
        assert_has_properties(g.node("ben"), props)

        assert g.node("hamza").node_type == "person"
        assert g.node("1") is not None


def test_add_edge():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        rg.add_edge(1, "ben", "hamza", properties=props, layer="friends")
        rg.add_edge(2, "ben", "hamza", layer="colleagues")
        rg.add_edge(3, "ben", "lucas")
        rg.add_edge(3, "shivam", "lucas", properties=props)

        g = client.receive_graph("path/to/event_graph")
        assert_has_properties(g.edge("ben", "hamza"), props)

        assert_set_eq(g.unique_layers, ["_default", "friends", "colleagues"])
        assert g.layer("friends").count_edges() == 1


def test_delete_edge():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")

        rg.add_edge(1, "ben", "hamza")
        rg.delete_edge(2, "ben", "hamza")
        g = client.receive_graph("path/to/event_graph")
        assert g.edge("ben", "hamza").history() == [1]
        assert g.edge("ben", "hamza").deletions() == [2]

        client.new_graph("path/to/persistent_graph", "PERSISTENT")
        rg = client.remote_graph("path/to/persistent_graph")
        rg.add_edge(1, "ben", "hamza")
        rg.delete_edge(2, "ben", "hamza")
        rg.add_edge(1, "ben", "lucas", layer="colleagues")
        rg.delete_edge(2, "ben", "lucas", layer="colleagues")
        g = client.receive_graph("path/to/persistent_graph")
        assert g.edge("ben", "hamza").deletions() == [2]
        assert g.edge("ben", "lucas").deletions() == [2]

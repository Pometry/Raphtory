import tempfile
from datetime import datetime, timezone
import pytest
from dateutil import parser
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


def helper_test_props(entity, props):
    for k, v in props.items():
        if isinstance(v, datetime):
            actual = parser.parse(entity.properties.get(k))
            assert v == actual
        else:
            assert entity.properties.get(k) == v


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
        helper_test_props(g.node("ben"), props)
        check_arr(g.node("ben").history(), [1, 2, 3, 4])


def test_add_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        node = rg.add_node(1, "ben")
        node.add_constant_properties(props)
        g = client.receive_graph("path/to/event_graph")
        helper_test_props(g.node("ben"), props)

        with pytest.raises(Exception) as excinfo:
            rg.node("ben").add_constant_properties({"prop_float": 3.0})
        assert "Tried to mutate constant property prop_float" in str(excinfo.value)


def test_update_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        node = rg.add_node(1, "ben")
        node.update_constant_properties(props)
        g = client.receive_graph("path/to/event_graph")
        helper_test_props(g.node("ben"), props)

        rg.node("ben").update_constant_properties({"prop_float": 3.0})
        g = client.receive_graph("path/to/event_graph")
        assert g.node("ben").properties.get("prop_float") == 3.0

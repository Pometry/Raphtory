import tempfile
from datetime import datetime, timezone
from typing import List
from dateutil import parser

from raphtory.graphql import (
    GraphServer,
    RaphtoryClient,
    RemoteGraph,
    RemoteUpdate,
    RemoteNodeAddition,
)


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


def create_updates(timestamps: List[int]):
    for time in timestamps:
        if time % 2 == 1:
            update = RemoteUpdate(time, make_props())
        else:
            update = RemoteUpdate(time, make_props2())
        yield update


def test_add_node():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg: RemoteGraph = client.remote_graph("path/to/event_graph")
        node_updates = []

        # add node with all fields
        ben_updates = list(create_updates([1, 2, 3, 4]))
        node_updates.append(
            RemoteNodeAddition("ben", "person", make_props(), ben_updates)
        )
        # follow up with only timestamp updates
        node_updates.append(
            RemoteNodeAddition(
                "ben", node_type=None, updates=[RemoteUpdate(5), RemoteUpdate(6)]
            )
        )
        # follow up with nothing
        node_updates.append(RemoteNodeAddition("ben"))
        # Start with just updates
        node_updates.append(
            RemoteNodeAddition("hamza", updates=[RemoteUpdate(1), RemoteUpdate(2)])
        )
        # Start with just updates
        node_updates.append(RemoteNodeAddition("lucas", updates=[RemoteUpdate(1)]))
        # add constant properties
        lucas_props = make_props()
        node_updates.append(
            RemoteNodeAddition("lucas", constant_properties=lucas_props)
        )
        # add node_type
        node_updates.append(RemoteNodeAddition("lucas", node_type="person"))
        rg.add_nodes(node_updates)
        g = client.receive_graph("path/to/event_graph")
        ben = g.node("ben")
        hamza = g.node("hamza")
        lucas = g.node("lucas")
        assert ben.properties.temporal.get("prop_float").values() == [
            2.0,
            3.0,
            2.0,
            3.0,
        ]
        assert ben.history() == [1, 2, 3, 4, 5, 6]
        assert ben.node_type == "person"
        assert hamza.history() == [1, 2]
        assert hamza.node_type is None
        helper_test_props(lucas, lucas_props)
        assert lucas.node_type == "person"

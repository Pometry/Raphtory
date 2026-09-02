from raphtory import Graph, PersistentGraph
from raphtory import EventTime
from raphtory import filter
import pytest
from datetime import datetime


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_graph(GraphClass):
    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    weight_lt3 = filter.ExplodedEdge.property("weight") < 3
    name_bob = filter.ExplodedEdge.property("name") == "bob"

    f_g = g.filter(filter=weight_e3)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions == []
        assert e2.deletions == []
    else:
        assert e1.deletions.t == [1, 2]
        assert e2.deletions.t == [1, 2]
        assert e1.deletions == [(1, 0), (2, 1)]
        assert e2.deletions == [(1, 3), (2, 4)]

    assert e1.history.t.collect() == [3]
    assert e2.history.t.collect() == [3]

    # assert e2.layer_names == ["red"] returning red blue for PersistentGraph which feels wrong?

    assert e1.properties.temporal.get("weight").items() == [(EventTime(3, 2), 3)]
    assert e2.properties.temporal.get("weight").items() == [(EventTime(3, 5), 3)]

    f_g = g.filter(filter=weight_lt3 & name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions == []
        assert e2.deletions == []
    else:
        assert e1.deletions.t == [2, 3]
        assert e2.deletions.t == [2, 3]

    assert e1.history.t.collect() == [1]
    assert e2.history.t.collect() == [1]

    # assert e2.layer_names == ["blue"] returning red blue for PersistentGraph which feels wrong?

    assert e1.properties.temporal.get("weight").items() == [(EventTime(1, 0), 1)]
    assert e2.properties.temporal.get("weight").items() == [(EventTime(1, 3), 1)]

    f_g = g.filter(filter=weight_e3 | name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions == []
        assert e2.deletions == []
    else:
        assert e1.deletions == [(2, 1)]
        assert e2.deletions == [(2, 4)]

    assert list(e1.history.t) == [1, 3]
    assert list(e2.history.t) == [1, 3]

    assert e2.layer_names == ["blue", "red"]

    assert e1.properties.temporal.get("weight").items() == [
        (EventTime(1, 0), 1),
        (EventTime(3, 2), 3),
    ]
    assert e2.properties.temporal.get("weight").items() == [
        (EventTime(1, 3), 1),
        (EventTime(3, 5), 3),
    ]


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_same_time_event(GraphClass):
    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    weight_lt3 = filter.ExplodedEdge.property("weight") < 3
    name_bob = filter.ExplodedEdge.property("name") == "bob"

    f_g = g.filter(filter=weight_lt3 & name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions == []
        assert e2.deletions == []
    else:
        assert e1.deletions == [(1, 1), (1, 2)]
        assert e2.deletions == [(1, 4), (1, 5)]

    assert list(e1.history.t) == [1]
    assert list(e2.history.t) == [1]

    # assert e2.layer_names == ["blue"] returning red blue which seems wrong

    assert e1.properties.temporal.get("weight").items() == [(EventTime(1, 0), 1)]
    assert e2.properties.temporal.get("weight").items() == [(EventTime(1, 3), 1)]

    f_g = g.filter(filter=weight_e3 | name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions == []
        assert e2.deletions == []
    else:
        assert e1.deletions.t == [1]
        assert e2.deletions.t == [1]

    assert list(e1.history.t) == [1, 1]
    assert list(e2.history.t) == [1, 1]

    assert e2.layer_names == ["blue", "red"]

    assert e1.properties.temporal.get("weight").items() == [
        (EventTime(1, 0), 1),
        (EventTime(1, 2), 3),
    ]
    assert e2.properties.temporal.get("weight").items() == [
        (EventTime(1, 3), 1),
        (EventTime(1, 5), 3),
    ]


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_with_edge_node_filter(GraphClass):
    g = GraphClass()
    g.add_edge(
        timestamp=1, src=1, dst=2, layer="blue", properties={"weight": 1, "name": "bob"}
    )
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    name_filter = filter.Node.name() == "2"

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(weight_e3 | name_filter).edges.explode()
    ]
    expected = [("1", "2"), ("1", "2"), ("1", "2"), ("1", "3"), ("1", "3"), ("1", "3")]
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(name_filter | weight_e3).edges.explode()
    ]
    expected = [("1", "2"), ("1", "2"), ("1", "2"), ("1", "3"), ("1", "3"), ("1", "3")]
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(weight_e3 & name_filter).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(name_filter & weight_e3).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name) for edge in g.filter(name_filter).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_all_property_types(GraphClass):
    g = GraphClass()

    g.add_edge(
        timestamp=1,
        src=1,
        dst=2,
        layer="blue",
        properties={
            "weight": 1,
            "confidence": 0.95,
            "name": "bob",
            "active": True,
            "created": datetime(2023, 1, 1),
            "tags": ["friend", "colleague"],
            "meta": {"role": "engineer"},
        },
    )

    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={
            "weight": 2,
            "confidence": 0.85,
            "name": "dave",
            "active": False,
            "created": datetime(2023, 5, 1),
            "tags": ["project_x"],
            "meta": {"role": "manager"},
        },
    )

    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={
            "weight": 3,
            "confidence": 0.75,
            "name": "greg",
            "active": True,
            "created": datetime(2024, 1, 15),
            "tags": [],
            "meta": {},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={
            "weight": 1,
            "confidence": 0.92,
            "name": "bob",
            "active": True,
            "created": datetime(2023, 3, 14),
            "tags": ["team_a"],
            "meta": {"location": "NYC"},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={
            "weight": 2,
            "confidence": 0.88,
            "name": "dave",
            "active": False,
            "created": datetime(2024, 6, 10),
            "tags": ["team_b", "remote"],
            "meta": {"location": "SF", "level": 2},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="red",
        properties={
            "weight": 3,
            "confidence": 0.80,
            "name": "greg",
            "active": True,
            "created": datetime(2025, 1, 1),
            "tags": ["consultant"],
            "meta": {"contract": True},
        },
    )

    test_cases = [
        # weight (int)
        (filter.ExplodedEdge.property("weight") == 2, 2),
        (filter.ExplodedEdge.property("weight") != 3, 4),
        (filter.ExplodedEdge.property("weight") < 3, 4),
        (filter.ExplodedEdge.property("weight") > 1, 4),
        (filter.ExplodedEdge.property("weight") <= 2, 4),
        (filter.ExplodedEdge.property("weight") >= 3, 2),
        (filter.ExplodedEdge.property("weight").is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").is_some(), 6),
        (filter.ExplodedEdge.property("weight").is_none(), 0),
        (
            filter.ExplodedEdge.property("weight").is_in(["1", 2]),
            4,
        ),  # numeric strings coerce to the property type
        (
            filter.ExplodedEdge.property("weight").is_not_in(["3"]),
            4,
        ),  # numeric strings coerce to the property type
        # confidence (float)
        (filter.ExplodedEdge.property("confidence") == 0.95, 1),
        (filter.ExplodedEdge.property("confidence") != 0.80, 5),
        (filter.ExplodedEdge.property("confidence") < 0.9, 4),
        (filter.ExplodedEdge.property("confidence") > 0.75, 5),
        (filter.ExplodedEdge.property("confidence") <= 0.85, 3),
        (filter.ExplodedEdge.property("confidence") >= 0.88, 3),
        (filter.ExplodedEdge.property("confidence").is_in([0.95, 0.92]), 2),
        (filter.ExplodedEdge.property("confidence").is_not_in([0.75]), 5),
        (filter.ExplodedEdge.property("confidence").is_some(), 6),
        (filter.ExplodedEdge.property("confidence").is_none(), 0),
        (
            filter.ExplodedEdge.property("confidence").is_in(["1", 0.95]),
            1,
        ),  # actually does the filter
        (
            filter.ExplodedEdge.property("confidence").is_not_in(["3", 0.95]),
            5,
        ),  # actually does the filter
        # name (str)
        (filter.ExplodedEdge.property("name") == "bob", 2),
        (filter.ExplodedEdge.property("name") != "greg", 4),
        (filter.ExplodedEdge.property("name").is_in(["bob", "dave"]), 4),
        (filter.ExplodedEdge.property("name").is_not_in(["greg"]), 4),
        (filter.ExplodedEdge.property("name").contains("bo"), 2),
        (filter.ExplodedEdge.property("name").not_contains("eg"), 4),
        (filter.ExplodedEdge.property("name").is_some(), 6),
        (filter.ExplodedEdge.property("name").is_none(), 0),
        (filter.ExplodedEdge.property("name") < "dave", 2),
        (filter.ExplodedEdge.property("name") > "dave", 2),
        (filter.ExplodedEdge.property("name") <= "dave", 4),
        (filter.ExplodedEdge.property("name") >= "dave", 4),
        (filter.ExplodedEdge.property("name").fuzzy_search("gabe", 2, False), 2),
        # active (bool)
        (filter.ExplodedEdge.property("active") == True, 4),
        (filter.ExplodedEdge.property("active") != False, 4),
        (filter.ExplodedEdge.property("active").is_in([True]), 4),
        (filter.ExplodedEdge.property("active").is_in([True, False]), 6),
        (filter.ExplodedEdge.property("active").is_not_in([False]), 4),
        (filter.ExplodedEdge.property("active").is_some(), 6),
        (filter.ExplodedEdge.property("active").is_none(), 0),
        # created (datetime)
        (filter.ExplodedEdge.property("created") == datetime(2023, 1, 1), 1),
        (filter.ExplodedEdge.property("created") != datetime(2023, 1, 1), 5),
        (filter.ExplodedEdge.property("created") < datetime(2024, 1, 1), 3),
        (filter.ExplodedEdge.property("created") > datetime(2024, 1, 1), 3),
        (filter.ExplodedEdge.property("created") <= datetime(2023, 5, 1), 3),
        (filter.ExplodedEdge.property("created") >= datetime(2024, 1, 15), 3),
        (
            filter.ExplodedEdge.property("created").is_in(
                [datetime(2023, 1, 1), datetime(2024, 6, 10)]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("created").is_not_in(
                [datetime(2024, 6, 10), datetime(2025, 1, 1)]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("created").is_some(), 6),
        (filter.ExplodedEdge.property("created").is_none(), 0),
        # tags (list of str)
        (filter.ExplodedEdge.property("tags") == ["team_b", "remote"], 1),
        (filter.ExplodedEdge.property("tags") != ["team_b", "remote"], 5),
        (
            filter.ExplodedEdge.property("tags").is_in(
                [["team_b", "remote"], ["team_a"]]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("tags").is_not_in(
                [["team_b", "remote"], ["team_a"]]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("tags").is_some(), 6),
        (filter.ExplodedEdge.property("tags").is_none(), 0),
        # meta (dict)
        (filter.ExplodedEdge.property("meta") == {"location": "SF", "level": 2}, 1),
        (filter.ExplodedEdge.property("meta") != {"location": "SF", "level": 2}, 5),
        (
            filter.ExplodedEdge.property("meta").is_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("meta").is_not_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("meta").is_some(), 6),
        (filter.ExplodedEdge.property("meta").is_none(), 0),
        (
            filter.ExplodedEdge.property("meta").is_not_in(
                [2, 4, {"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (
            filter.ExplodedEdge.property("meta").is_in(
                ["hi", {"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
    ]
    print()
    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    # Ordering operators and non-boolean set values are rejected for boolean
    # properties.
    for make_bad in (
        lambda: filter.ExplodedEdge.property("active") < True,
        lambda: filter.ExplodedEdge.property("active") >= False,
        lambda: filter.ExplodedEdge.property("active").is_in([1, 2]),
        lambda: filter.ExplodedEdge.property("active").is_not_in([3]),
        lambda: filter.ExplodedEdge.property("name").is_in([1, 2]),
        lambda: filter.ExplodedEdge.property("name").is_not_in([3, "dave"]),
        lambda: filter.ExplodedEdge.property("created").is_in([1, 2]),
        lambda: filter.ExplodedEdge.property("created").is_not_in([3]),
        lambda: filter.ExplodedEdge.property("tags").is_in([1, 2]),
        lambda: filter.ExplodedEdge.property("tags").is_in([1, 2, ["team_a", 0]]),
        lambda: filter.ExplodedEdge.property("tags").is_not_in([3]),
    ):
        with pytest.raises(
            Exception, match=r"not valid for boolean properties|cannot be coerced"
        ):
            g.filter(make_bad()).edges.explode()

    nonsense_filter_cases = [
        # Integers (weight)
        (
            lambda: filter.ExplodedEdge.property("weight").contains(2),
            "is not a valid string operand",
        ),
        (
            lambda: filter.ExplodedEdge.property("weight").not_contains(3),
            "is not a valid string operand",
        ),
        (
            lambda: filter.ExplodedEdge.property("weight").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        # Floats (confidence)
        (
            lambda: filter.ExplodedEdge.property("confidence").contains(0.9),
            "is not a valid string operand",
        ),
        (
            lambda: filter.ExplodedEdge.property("confidence").not_contains(0.8),
            "is not a valid string operand",
        ),
        (
            lambda: filter.ExplodedEdge.property("confidence").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        # Booleans (active)
        (
            lambda: filter.ExplodedEdge.property("active").contains(True),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("active").not_contains(False),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("active").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        # Datetimes (created)
        (
            lambda: filter.ExplodedEdge.property("created").contains(datetime(2023, 1, 1)),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("created").not_contains(datetime(2023, 1, 1)),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("created").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        # Lists (tags) — odd comparisons
        (
            lambda: filter.ExplodedEdge.property("tags").contains("team_a"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags").not_contains("team_z"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") < ["x"],
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") > ["a"],
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") <= ["team_b"],
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") >= ["consultant"],
            "not valid for list properties",
        ),
        # Dicts (meta) — contains() expects a key, but here simulates wrong context
        (
            lambda: filter.ExplodedEdge.property("meta").contains("role"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta").not_contains("salary"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta").fuzzy_search("blah", 2, False),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta")
            < {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta")
            < {"location": "SF", "level": 2, "role": "blah"},
            "not valid for map properties",
        ),  # check subset of keys also raise the same error
        (
            lambda: filter.ExplodedEdge.property("meta")
            <= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta")
            > {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta")
            >= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "not valid for map properties",
        ),
    ]

    for i, (make_expr, message) in enumerate(nonsense_filter_cases):
        with pytest.raises(Exception) as e:
            print(len(g.filter(make_expr()).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    # Numeric strings coerce to the property's numeric type: each string form
    # matches exactly what its native-typed twin matches.
    for prop, val in (("weight", 2), ("weight", 3), ("confidence", 2)):
        for op in ("__eq__", "__ne__", "__lt__", "__gt__", "__le__", "__ge__"):
            typed = getattr(filter.ExplodedEdge.property(prop), op)(val)
            coerced = getattr(filter.ExplodedEdge.property(prop), op)(str(val))
            assert len(g.filter(coerced).edges.explode()) == len(
                g.filter(typed).edges.explode()
            ), prop + " " + op + " " + str(val)

    wrong_types = [
        # Integers (weight)
        (
            lambda: filter.ExplodedEdge.property("weight").contains("bo"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("weight").not_contains("eg"),
            "string operator requires a Str property",
        ),
        # Floats (confidence)
        (
            lambda: filter.ExplodedEdge.property("confidence").contains("bo"),
            "string operator requires a Str property",
        ),
        (
            lambda: filter.ExplodedEdge.property("confidence").not_contains("eg"),
            "string operator requires a Str property",
        ),
        # # Strings (name)
        (
            lambda: filter.ExplodedEdge.property("name") == 2,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name") != 3,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name") < 3,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name") > 1,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name") <= 2,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name") >= 3,
            "cannot be coerced to Str",
        ),
        (
            lambda: filter.ExplodedEdge.property("name").contains(2),
            "is not a valid string operand",
        ),
        (
            lambda: filter.ExplodedEdge.property("name").not_contains(3),
            "is not a valid string operand",
        ),
        # Booleans (active)
        (
            lambda: filter.ExplodedEdge.property("active") == 2,
            "cannot be coerced to Bool",
        ),
        (
            lambda: filter.ExplodedEdge.property("active") != 3,
            "cannot be coerced to Bool",
        ),
        (
            lambda: filter.ExplodedEdge.property("active") < 3,
            "not valid for boolean properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("active") > 1,
            "not valid for boolean properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("active") <= 2,
            "not valid for boolean properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("active") >= 3,
            "not valid for boolean properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("active").contains(2),
            "is not a valid string operand",
        ),  # should fail on contains not type
        (
            lambda: filter.ExplodedEdge.property("active").not_contains(3),
            "is not a valid string operand",
        ),  # should fail on contains not type
        # # Datetimes (created)
        (
            lambda: filter.ExplodedEdge.property("created") == 2,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created") != 3,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created") < 3,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created") > 1,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created") <= 2,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created") >= 3,
            "cannot be coerced to NDTime",
        ),
        (
            lambda: filter.ExplodedEdge.property("created").contains(2),
            "is not a valid string operand",
        ),  # should fail on contains not type
        (
            lambda: filter.ExplodedEdge.property("created").not_contains(3),
            "is not a valid string operand",
        ),  # should fail on contains not type
        # # Lists (tags)
        (
            lambda: filter.ExplodedEdge.property("tags") == 2,
            "cannot be coerced to List",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") != 3,
            "cannot be coerced to List",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") < 3,
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") > 1,
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") <= 2,
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags") >= 3,
            "not valid for list properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("tags").contains(2),
            "is not a valid string operand",
        ),  # should fail on contains not type
        (
            lambda: filter.ExplodedEdge.property("tags").not_contains(3),
            "is not a valid string operand",
        ),  # should fail on contains not type
        # # Dicts (meta)
        (
            lambda: filter.ExplodedEdge.property("meta") == 2,
            "cannot be coerced to Map",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta") != 3,
            "cannot be coerced to Map",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta") < 3,
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta") > 1,
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta") <= 2,
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta") >= 3,
            "not valid for map properties",
        ),
        (
            lambda: filter.ExplodedEdge.property("meta").contains(2),
            "is not a valid string operand",
        ),  # should fail on contains not type
        (
            lambda: filter.ExplodedEdge.property("meta").not_contains(3),
            "is not a valid string operand",
        ),  # should fail on contains not type
    ]

    for i, (make_expr, message) in enumerate(wrong_types):
        with pytest.raises(Exception) as e:
            print(len(g.filter(make_expr()).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    with pytest.raises(Exception) as e:
        filter.ExplodedEdge.property("name").fuzzy_search(2, 2, False)
    assert "is not a valid string operand" in str(e.value)

    missing_prop = [
        (lambda: filter.ExplodedEdge.property("blah") == 2),
        (lambda: filter.ExplodedEdge.property("blah") != 3),
        (lambda: filter.ExplodedEdge.property("blah") < 3),
        (lambda: filter.ExplodedEdge.property("blah") > 1),
        (lambda: filter.ExplodedEdge.property("blah") <= 2),
        (lambda: filter.ExplodedEdge.property("blah") >= 3),
        (lambda: filter.ExplodedEdge.property("blah").is_in([1, 2])),
        (lambda: filter.ExplodedEdge.property("blah").is_not_in([3])),
        (lambda: filter.ExplodedEdge.property("blah").contains(["blah"])),
        (lambda: filter.ExplodedEdge.property("blah").contains([])),
        (lambda: filter.ExplodedEdge.property("blah").not_contains([])),
        (lambda: filter.ExplodedEdge.property("blah").not_contains(["blah"])),
        (lambda: filter.ExplodedEdge.property("blah").is_some()),
        (lambda: filter.ExplodedEdge.property("blah").is_none()),
    ]

    for make_expr in missing_prop:
        with pytest.raises(Exception) as e:
            # force evaluation so the exception surfaces here
            _ = g.filter(make_expr()).edges.explode()
        # mistyped operands may fail at construction before the property
        # lookup happens
        assert "Property blah does not exist" in str(
            e.value
        ) or "is not a valid string operand" in str(e.value)


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_temporal_constant(GraphClass):
    g = GraphClass()
    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={"weight": 1, "name": "bob", "p20": "Gold_ship"},
    )
    g.add_edge(
        2,
        1,
        2,
        layer="blue",
        properties={"weight": 2, "name": "dave", "p20": "Gold_ship"},
    )
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={"weight": 1, "name": "bob", "p20": "Old_boat"},
    )
    g.add_edge(
        2,
        1,
        3,
        layer="blue",
        properties={"weight": 2, "name": "dave", "p20": "Gold_ship"},
    )
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    # Temporal shoudl act exactly the same as non-temporal
    test_cases = [
        (filter.ExplodedEdge.property("weight").temporal().any() == 2, 2),
        (filter.ExplodedEdge.property("weight").temporal().any() != 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() < 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() > 1, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() <= 2, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() >= 3, 2),
        (filter.ExplodedEdge.property("weight").temporal().any().is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").temporal().any().is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").temporal().any().is_some(), 6),
        (filter.ExplodedEdge.property("weight").temporal().any().is_none(), 0),
        (filter.ExplodedEdge.property("weight").temporal().last() == 2, 2),
        (filter.ExplodedEdge.property("weight").temporal().last() != 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() < 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() > 1, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() <= 2, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() >= 3, 2),
        (filter.ExplodedEdge.property("weight").temporal().last().is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").temporal().last().is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").temporal().last().is_some(), 6),
        (filter.ExplodedEdge.property("weight").temporal().last().is_none(), 0),
        (filter.ExplodedEdge.property("p20").temporal().first().starts_with("Old"), 1),
        (filter.ExplodedEdge.property("p20").temporal().first().ends_with("boat"), 1),
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue")
    g.add_edge(2, 1, 2, layer="blue")
    e = g.add_edge(3, 1, 2, layer="blue")
    e.add_metadata(metadata={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue")
    g.add_edge(2, 1, 3, layer="blue")
    e = g.add_edge(3, 1, 3, layer="blue")
    e.add_metadata(metadata={"weight": 2, "name": "dave"})

    test_cases = [
        (filter.Edge.metadata("weight") == 2, 3),
        (filter.Edge.metadata("weight") != 3, 6),
        (filter.Edge.metadata("weight") < 3, 6),
        (filter.Edge.metadata("weight") > 1, 3),
        (filter.Edge.metadata("weight") <= 2, 6),
        (filter.Edge.metadata("weight") >= 3, 0),
        (filter.Edge.metadata("weight").is_in([1, 2]), 6),
        (filter.Edge.metadata("weight").is_not_in([3]), 6),
        (filter.Edge.metadata("weight").is_some(), 6),
        (filter.Edge.metadata("weight").is_none(), 0),
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        print(g.edges.explode().metadata.get("weight"))
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

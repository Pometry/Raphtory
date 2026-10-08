# Time travel

In a plain RDF store a triple is either in the store or not. In Raphtory every triple has a history: loading a document
asserts its triples at a time, retracting a document retracts them at a later time, and every assertion and retraction
is kept. You can then ask what was true at any point in time, in two ways:

- pick a view of the graph, such as `g.snapshot_at(t)`, and query or export it as usual;
- name the times inside a SPARQL query with the time graphs `<raphtory:asof:T>`, to compare several times in one query.

This page uses a `PersistentGraph`, in which a triple holds from the time it is asserted until the time it is retracted.
The differences on an event `Graph` are covered [below](#event-graphs-and-persistent-graphs).

## A history of employment

The examples on this page use the following history. Alice joins Acme in June 2021 and Bob joins in March 2022. In June
2023 Alice leaves Acme for Initech, which is recorded as the retraction of one triple and the assertion of another at the
same time:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


g = PersistentGraph()
g.load_rdf("2021-06-01", works_for("alice", "acme"))
g.load_rdf("2022-03-01", works_for("bob", "acme"))
g.retract_rdf("2023-06-01", works_for("alice", "acme"))
g.load_rdf("2023-06-01", works_for("alice", "initech"))
```
///

## Querying the past with views

A SPARQL query sees the triples that are visible in the view it runs on. On a `PersistentGraph`, the graph itself shows
the current state, and [snapshot_at()][raphtory.GraphView.snapshot_at] shows the state as of a time:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
query = """
    SELECT ?person ?company WHERE { ?person <http://example.org/worksFor> ?company }
    ORDER BY ?person
"""
print("now       ", g.sparql(query))
for date in ["2021-01-01", "2022-01-01", "2023-01-01"]:
    print(date, g.snapshot_at(date).sparql(query))
```
///

```{.python continuation hide}
alice_acme = {"person": "http://example.org/alice", "company": "http://example.org/acme"}
alice_initech = {"person": "http://example.org/alice", "company": "http://example.org/initech"}
bob_acme = {"person": "http://example.org/bob", "company": "http://example.org/acme"}
assert g.sparql(query) == [alice_initech, bob_acme]
assert g.snapshot_at("2021-01-01").sparql(query) == []
assert g.snapshot_at("2022-01-01").sparql(query) == [alice_acme]
assert g.snapshot_at("2023-01-01").sparql(query) == [alice_acme, bob_acme]
assert g.snapshot_at("2023-06-01").sparql(query) == [alice_initech, bob_acme]
```

!!! Output

    ```output
    now        [{'person': 'http://example.org/alice', 'company': 'http://example.org/initech'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    2021-01-01 []
    2022-01-01 [{'person': 'http://example.org/alice', 'company': 'http://example.org/acme'}]
    2023-01-01 [{'person': 'http://example.org/alice', 'company': 'http://example.org/acme'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    ```

The same views work with [to_rdf()][raphtory.GraphView.to_rdf], so `g.snapshot_at("2023-01-01").to_rdf()` exports the
triples that held at the start of 2023.

Each triple is judged on its own events. On a `PersistentGraph` the time views behave as follows:

| View                                    | A triple is visible if                                                             |
|-----------------------------------------|------------------------------------------------------------------------------------|
| `g`, `g.after(t)`, `g.snapshot_latest()` | its latest event is an assertion: the current state                                |
| `g.snapshot_at(t)`                      | its latest event at or before `t` is an assertion: the state as of `t`             |
| `g.before(t)`, `g.window(start, t)`     | its latest event before `t` is an assertion: the state just before `t`             |

The views in the first row show the same triples, but only `g` itself has no time window. `g.after(t)` is a window
that starts after `t`, and `g.snapshot_latest()`, like `g.snapshot_at(t)`, is a window of a single instant. This
matters for the [time graphs](#comparing-times-in-one-query) described below, which are empty outside the window of the
view they are queried on.

A window answers "what held at the end of the window", not "what held at some point during the window": the start of a
window makes no difference. In the example, Alice worked for Acme during the first half of 2023, but a window over 2023
only shows where she works at the end of it:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(g.window("2023-01-01", "2024-01-01").sparql(query))
```
///

```{.python continuation hide}
assert g.window("2023-01-01", "2024-01-01").sparql(query) == [alice_initech, bob_acme]
```

!!! Output

    ```output
    [{'person': 'http://example.org/alice', 'company': 'http://example.org/initech'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    ```

## How assertions and retractions combine

Assertions and retractions are ordered by their time, not by the order in which they were loaded. The rules are:

- A triple holds from an assertion until the next retraction, and holds again from a later assertion.
- If an assertion and a retraction of the same triple have the same time, the one that was written last wins. So
  `load_rdf(t, ...)` followed by `retract_rdf(t, ...)` leaves the triple retracted at `t`, and the opposite order leaves
  it asserted.
- Asserting a triple that already holds does not change what is visible (a query still returns it once), but the event
  is kept in its history. One retraction ends the triple, however many times it was asserted before.
- Retracting a triple that was never asserted is recorded, but the triple never becomes visible.
- Retracting a triple does not affect other triples between the same two nodes, because each predicate is a separate
  layer.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

triple = b"<http://example.org/a> <http://example.org/p> <http://example.org/b> ."
ask = "ASK { <http://example.org/a> <http://example.org/p> <http://example.org/b> }"

# loaded out of order: the retraction at time 5 is loaded before the assertion at time 1
out_of_order = PersistentGraph()
out_of_order.retract_rdf(5, triple)
out_of_order.load_rdf(1, triple)
print([out_of_order.snapshot_at(t).sparql(ask) for t in range(7)])

# the same time: the call made last wins
same_time = PersistentGraph()
same_time.load_rdf(1, triple)
same_time.retract_rdf(1, triple)
print(same_time.snapshot_at(1).sparql(ask))

# a retraction of a triple that was never asserted creates its nodes, but is never visible
never_asserted = PersistentGraph()
never_asserted.retract_rdf(1, triple)
print(never_asserted.count_nodes(), never_asserted.sparql(ask))
```
///

```{.python continuation hide}
assert [out_of_order.snapshot_at(t).sparql(ask) for t in range(7)] == [False, True, True, True, True, False, False]
assert out_of_order.sparql(ask) is False
assert same_time.snapshot_at(1).sparql(ask) is False

# the opposite order leaves the triple asserted
reversed_order = PersistentGraph()
reversed_order.retract_rdf(1, triple)
reversed_order.load_rdf(1, triple)
assert reversed_order.snapshot_at(1).sparql(ask) is True

# duplicate assertions are one row, and one retraction ends them
duplicates = PersistentGraph()
duplicates.load_rdf(1, triple)
duplicates.load_rdf(2, triple)
duplicates.retract_rdf(3, triple)
assert len(duplicates.snapshot_at(2).sparql("SELECT * WHERE { ?s ?p ?o }")) == 1
assert duplicates.snapshot_at(3).sparql(ask) is False

assert never_asserted.count_nodes() == 2
assert never_asserted.sparql(ask) is False
assert never_asserted.snapshot_at(1).sparql(ask) is False
assert never_asserted.window(0, 10).sparql(ask) is False
```

!!! Output

    ```output
    [False, True, True, True, True, False, False]
    False
    2 False
    ```

### The history of a triple

Since a triple is an edge in a layer, Raphtory's usual edge API gives its full history. The edge's
[history][raphtory.Edge.history] holds the times it was asserted and its [deletions][raphtory.Edge.deletions] hold the
times it was retracted:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


g = PersistentGraph()
g.load_rdf("2021-06-01", works_for("alice", "acme"))
g.retract_rdf("2023-06-01", works_for("alice", "acme"))

triple = g.layer("http://example.org/worksFor").edge("http://example.org/alice", "http://example.org/acme")
print(triple.history.dt.collect())
print(triple.deletions.dt.collect())
```
///

```{.python continuation hide}
from datetime import datetime, timezone

assert triple.history.dt.collect() == [datetime(2021, 6, 1, tzinfo=timezone.utc)]
assert triple.deletions.dt.collect() == [datetime(2023, 6, 1, tzinfo=timezone.utc)]
```

!!! Output

    ```output
    [datetime.datetime(2021, 6, 1, 0, 0, tzinfo=datetime.timezone.utc)]
    [datetime.datetime(2023, 6, 1, 0, 0, tzinfo=datetime.timezone.utc)]
    ```

## Since when? Validity functions

The edge API gives the whole history of a triple. Inside a query, four functions answer the common question "since when
has this triple held, and until when?":

| Function                          | Returns                                                                           |
|-----------------------------------|-----------------------------------------------------------------------------------|
| `raphtory:validFrom(s, p, o)`     | the time since which the triple has held, as an `xsd:dateTime` in UTC              |
| `raphtory:validTo(s, p, o)`       | the time it stopped holding (the retraction that ended it), as an `xsd:dateTime`   |
| `raphtory:validFromTime(s, p, o)` | the same time as `validFrom`, as a Raphtory time (milliseconds since the epoch)    |
| `raphtory:validToTime(s, p, o)`   | the same time as `validTo`, as a Raphtory time                                     |

With `decode_literals=True`, the date-times are returned as Python `datetime` values and the Raphtory times as `int`.
This query lists the current jobs of the [history above](#a-history-of-employment), with their start dates:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


g = PersistentGraph()
g.load_rdf("2021-06-01", works_for("alice", "acme"))
g.load_rdf("2022-03-01", works_for("bob", "acme"))
g.retract_rdf("2023-06-01", works_for("alice", "acme"))
g.load_rdf("2023-06-01", works_for("alice", "initech"))

rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person ?company ?since WHERE {
        ?person ex:worksFor ?company
        BIND(raphtory:validFrom(?person, ex:worksFor, ?company) AS ?since)
    }
    ORDER BY ?since
""", decode_literals=True)
for row in rows:
    print(row)
```
///

```{.python continuation hide}
from datetime import datetime, timezone

assert rows == [
    {"person": "http://example.org/bob", "company": "http://example.org/acme", "since": datetime(2022, 3, 1, tzinfo=timezone.utc)},
    {"person": "http://example.org/alice", "company": "http://example.org/initech", "since": datetime(2023, 6, 1, tzinfo=timezone.utc)},
]
```

!!! Output

    ```output
    {'person': 'http://example.org/bob', 'company': 'http://example.org/acme', 'since': datetime.datetime(2022, 3, 1, 0, 0, tzinfo=datetime.timezone.utc)}
    {'person': 'http://example.org/alice', 'company': 'http://example.org/initech', 'since': datetime.datetime(2023, 6, 1, 0, 0, tzinfo=datetime.timezone.utc)}
    ```

Without a fourth argument the functions answer for the present of the view, so `validTo` is always unbound there: a
triple that is visible now has not stopped holding. To ask about another time, pass a *reference time* as the fourth
argument: a [time graph](#comparing-times-in-one-query) such as `raphtory:asof:2023-01-01`, an integer Raphtory time or
an `xsd:dateTime` (in UTC if it has no timezone). The functions then return the interval of validity that contains the
reference time:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person ?since ?until WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ex:acme }
        BIND(raphtory:validFrom(?person, ex:worksFor, ex:acme, raphtory:asof:2023-01-01) AS ?since)
        BIND(raphtory:validTo(?person, ex:worksFor, ex:acme, raphtory:asof:2023-01-01) AS ?until)
    }
    ORDER BY ?person
""", decode_literals=True)
for row in rows:
    print(row["person"], row["since"].date(), row["until"] and row["until"].date())
```
///

```{.python continuation hide}
assert rows == [
    {
        "person": "http://example.org/alice",
        "since": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "until": datetime(2023, 6, 1, tzinfo=timezone.utc),
    },
    {"person": "http://example.org/bob", "since": datetime(2022, 3, 1, tzinfo=timezone.utc), "until": None},
]
```

!!! Output

    ```output
    http://example.org/alice 2021-06-01 2023-06-01
    http://example.org/bob 2022-03-01 None
    ```

Functions cannot see the `GRAPH` or `FROM` they are called in: inside `GRAPH raphtory:asof:2023-01-01 { ... }`, a call
without a fourth argument still answers for the present. To follow the time graphs of a `GRAPH ?g` pattern, pass `?g` as
the reference time, in a `BIND` placed after the `GRAPH` pattern (inside the pattern, `?g` is not bound yet). This query
shows, at the start of 2022 and of 2023, when each job at Acme was going to end:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?asof ?person ?until
    FROM NAMED raphtory:asof:2022-01-01
    FROM NAMED raphtory:asof:2023-01-01
    WHERE {
        GRAPH ?asof { ?person ex:worksFor ex:acme }
        BIND(raphtory:validTo(?person, ex:worksFor, ex:acme, ?asof) AS ?until)
    }
    ORDER BY ?asof ?person
""", decode_literals=True)
for row in rows:
    print(row["asof"], row["person"], row["until"] and row["until"].date())
```
///

```{.python continuation hide}
june_2023 = datetime(2023, 6, 1, tzinfo=timezone.utc)
assert rows == [
    {"asof": "<raphtory:asof:2022-01-01>", "person": "http://example.org/alice", "until": june_2023},
    {"asof": "<raphtory:asof:2023-01-01>", "person": "http://example.org/alice", "until": june_2023},
    {"asof": "<raphtory:asof:2023-01-01>", "person": "http://example.org/bob", "until": None},
]
```

!!! Output

    ```output
    <raphtory:asof:2022-01-01> http://example.org/alice 2023-06-01
    <raphtory:asof:2023-01-01> http://example.org/alice 2023-06-01
    <raphtory:asof:2023-01-01> http://example.org/bob None
    ```

The rules are:

- **Only visible triples.** The functions return an unbound value (`None`) for a triple the view does not show, as of
  the reference time if there is one. So they never reveal triples hidden by layers, subgraphs, filters or time windows.
  On a view with a time window, such as `g.snapshot_at(t)`, a reference time before the start of the window gives
  `None`. On a `PersistentGraph` so does a reference time at or after the end of the window, while on an event `Graph`
  such a reference time answers as of the end of the window, like `GRAPH raphtory:asof:T` on the same view.
- **The whole history counts, up to the end of the view.** The interval is computed from every event of the triple
  before the end of the view: on `g.window(start, end)` an interval can begin before `start`, and on
  `g.snapshot_at(t)` the events after `t` are ignored, so `validTo` is `None` for a triple that still held at `t`.
- **Assertions and retractions** combine as [described above](#how-assertions-and-retractions-combine): asserting a
  triple that already holds does not restart its interval, an assertion and a retraction at the same time count as the
  one written last, and a triple that was only ever retracted never holds.
- **Event graphs.** On an event `Graph` a triple holds from its first assertion on, so `validTo` is always `None`. Use
  `g.persistent_graph()` to get intervals that end at retractions.
- **Literal objects** are matched by their canonical form, because SPARQL passes numbers, booleans and dates to
  functions in canonical form (`"042"^^xsd:integer` arrives as `42`): `42` finds the object `"042"^^xsd:integer` or
  `"42"^^xsd:int`, but a value of another datatype (`1` and `1.0`) or a date-time written with another timezone is not
  matched, although `FILTER` compares them as equal. Pass the object bound by the pattern. If the subject has several
  objects with that canonical form in the layer that the view shows, such as `"42"^^xsd:int` and
  `"042"^^xsd:integer`, the result is `None`.
- **No errors.** A wrong number of arguments, a term that is not in the graph, or a reference time that is not a time
  gives `None` instead of an error. A misspelled function name, such as `raphtory:validfrom`, makes the query fail.
- **Dates.** `validFrom` and `validTo` have millisecond precision and are `None` for a time outside the years 1 to 9999;
  `validFromTime` and `validToTime` still answer.
- **Names.** `raphtory:validFrom` is also the IRI of a node named `validFrom`. SPARQL tells a function call from a term
  by its position, so the two do not clash.

```{.python continuation hide}
import pytest
from raphtory import Graph

alice_acme = "<http://example.org/alice>, <http://example.org/worksFor>, <http://example.org/acme>"


def interval(view, at=None):
    args = alice_acme if at is None else alice_acme + ", " + at
    [row] = view.sparql(
        "SELECT (raphtory:validFromTime(" + args + ") AS ?since) (raphtory:validToTime(" + args + ") AS ?until) {}"
    )
    return row["since"], row["until"]


june_2021 = '"1622505600000"^^<http://www.w3.org/2001/XMLSchema#integer>'
june_2023_ms = '"1685577600000"^^<http://www.w3.org/2001/XMLSchema#integer>'
# now, and as of 2023 in every form of reference time
assert interval(g) == (None, None)
for at in [
    "raphtory:asof:2023-01-01",
    "1672531200000",
    '"2023-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
    '"2023-01-01T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>',
]:
    assert interval(g, at) == (june_2021, june_2023_ms), at
# references that are not times, and wrong numbers of arguments
assert interval(g, '"2023-01-01"') == (None, None)
assert g.sparql("SELECT (raphtory:validFrom(<http://example.org/alice>) AS ?x) {}") == [{"x": None}]
with pytest.raises(Exception, match="SPARQL evaluation error"):
    g.sparql("SELECT (raphtory:validfrom(" + alice_acme + ") AS ?x) {}")
# views: a reference outside the window, a window that starts after the interval, a snapshot before the retraction
assert interval(g.snapshot_at("2023-01-01"), "raphtory:asof:2022-01-01") == (None, None)
assert interval(g.window("2023-01-01", "2023-03-01")) == (june_2021, None)
assert interval(g.snapshot_at("2023-01-01")) == (june_2021, None)
assert interval(g.exclude_nodes(["http://example.org/acme"]), "raphtory:asof:2023-01-01") == (None, None)
# inside GRAPH, a call without a reference answers for the present
assert g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?since WHERE {
        GRAPH raphtory:asof:2023-01-01 {
            ex:alice ex:worksFor ex:acme BIND(raphtory:validFrom(ex:alice, ex:worksFor, ex:acme) AS ?since)
        }
    }
""") == [{"since": None}]
# event graphs: from the first assertion on
events = Graph()
events.load_rdf("2021-06-01", works_for("alice", "acme"))
events.retract_rdf("2023-06-01", works_for("alice", "acme"))
assert interval(events) == (june_2021, None)
assert interval(events.persistent_graph(), "raphtory:asof:2023-01-01") == (june_2021, june_2023_ms)
# a reference time after the end of the window: None on a persistent graph, the end of the window on an event graph
assert interval(g.window("2021-01-01", "2022-01-01"), "raphtory:asof:2023-01-01") == (None, None)
assert interval(events.window("2021-01-01", "2022-01-01"), "raphtory:asof:2023-01-01") == (june_2021, None)
# and one before the start of the window gives None there too
assert interval(events.window("2021-01-01", "2022-01-01"), "raphtory:asof:2020-01-01") == (None, None)
```

## Event graphs and persistent graphs

RDF can be loaded into both of Raphtory's [temporal graph representations](../persistent-graph/1_intro.md), and both
store exactly the same events. They differ in what a query sees:

- In a `PersistentGraph`, a triple holds from its assertion until its retraction, as described above. Use it for facts
  that stay true until they are retracted, which is what most RDF data describes.
- In an event `Graph`, every assertion is an instantaneous event. A query sees every triple that was asserted at least
  once in the view, and retractions are ignored. Use it when each triple records something that happened, such as
  "alice emailed bob".

| View on a `Graph`                      | A triple is visible if                    |
|----------------------------------------|-------------------------------------------|
| `g`                                    | it was ever asserted                      |
| `g.snapshot_at(t)`                     | it was asserted at or before `t`          |
| `g.window(start, end)`                 | it was asserted at least once in the window, from `start` up to but excluding `end` |

You can switch between the two representations of the same data at any time, without copying it, with
`g.persistent_graph()` and `g.event_graph()`:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


g = Graph()
g.load_rdf("2021-06-01", works_for("alice", "acme"))
g.retract_rdf("2023-06-01", works_for("alice", "acme"))
g.load_rdf("2023-06-01", works_for("alice", "initech"))

query = "SELECT ?company WHERE { <http://example.org/alice> <http://example.org/worksFor> ?company } ORDER BY ?company"
print(g.sparql(query))                                       # every assertion
print(g.window("2023-01-01", "2024-01-01").sparql(query))    # assertions during 2023
print(g.persistent_graph().sparql(query))                    # the current state
```
///

```{.python continuation hide}
assert g.sparql(query) == [{"company": "http://example.org/acme"}, {"company": "http://example.org/initech"}]
assert g.window("2023-01-01", "2024-01-01").sparql(query) == [{"company": "http://example.org/initech"}]
assert g.persistent_graph().sparql(query) == [{"company": "http://example.org/initech"}]
```

!!! Output

    ```output
    [{'company': 'http://example.org/acme'}, {'company': 'http://example.org/initech'}]
    [{'company': 'http://example.org/initech'}]
    [{'company': 'http://example.org/initech'}]
    ```

## Comparing times in one query

To compare several points in time in a single query, use the time graphs `<raphtory:asof:T>`. These are virtual,
read-only named graphs: `GRAPH <raphtory:asof:T> { ... }` matches its patterns against the triples visible as of `T`,
exactly as if the query were run on `g.snapshot_at(T)`. Because the `raphtory:` prefix is registered, you can write them
as `raphtory:asof:2024-01-01`.

`T` is a time in one of the formats below. A date or a date and time without a timezone is in UTC:

| Format                                      | Example                                       |
|---------------------------------------------|-----------------------------------------------|
| milliseconds since the Unix epoch           | `raphtory:asof:1704067200000`                 |
| a date (midnight UTC)                       | `raphtory:asof:2024-01-01`                    |
| a date and time                             | `raphtory:asof:2024-01-01T00:00:00`, `raphtory:asof:2024-01-01T00:00:00.000` |
| an RFC 3339 date and time with a timezone   | `raphtory:asof:2024-01-01T00:00:00Z`, `<raphtory:asof:2024-01-01T01:00:00+01:00>` |

A `+` cannot appear in a prefixed name, so write a time with a `+` offset as a full IRI in angle brackets, as in the last
example. A time that does not parse, such as `raphtory:asof:yesterday`, makes the query fail with an `invalid time
graph` error.

### Who changed employer?

This query finds the people whose employer at the start of 2024 is different from their employer at the start of 2023,
using the history defined at the [top of the page](#a-history-of-employment):

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


g = PersistentGraph()
g.load_rdf("2021-06-01", works_for("alice", "acme"))
g.load_rdf("2022-03-01", works_for("bob", "acme"))
g.retract_rdf("2023-06-01", works_for("alice", "acme"))
g.load_rdf("2023-06-01", works_for("alice", "initech"))

rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person ?before ?after WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ?before }
        GRAPH raphtory:asof:2024-01-01 { ?person ex:worksFor ?after }
        FILTER(?before != ?after)
    }
""")
print(rows)
```
///

```{.python continuation hide}
assert rows == [
    {"person": "http://example.org/alice", "before": "http://example.org/acme", "after": "http://example.org/initech"}
]
```

!!! Output

    ```output
    [{'person': 'http://example.org/alice', 'before': 'http://example.org/acme', 'after': 'http://example.org/initech'}]
    ```

Time graphs combine with the rest of SPARQL. For example, `FILTER NOT EXISTS` finds who joined Acme during 2022:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ex:acme }
        FILTER NOT EXISTS { GRAPH raphtory:asof:2022-01-01 { ?person ex:worksFor ex:acme } }
    }
"""))
```
///

```{.python continuation hide}
assert g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ex:acme }
        FILTER NOT EXISTS { GRAPH raphtory:asof:2022-01-01 { ?person ex:worksFor ex:acme } }
    }
""") == [{"person": "http://example.org/bob"}]
```

!!! Output

    ```output
    [{'person': 'http://example.org/bob'}]
    ```

### A series of dates

Time graphs cannot be listed, so `GRAPH ?g { ... }` on its own matches nothing. To visit several times with one pattern,
name them, for example with `FROM NAMED`, and match them with `GRAPH ?g`. This query counts the staff of Acme at the
start of each year:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?asof (COUNT(?person) AS ?staff)
    FROM NAMED raphtory:asof:2022-01-01
    FROM NAMED raphtory:asof:2023-01-01
    FROM NAMED raphtory:asof:2024-01-01
    WHERE { GRAPH ?asof { ?person ex:worksFor ex:acme } }
    GROUP BY ?asof ORDER BY ?asof
""", decode_literals=True)
for row in rows:
    print(row)
```
///

```{.python continuation hide}
assert rows == [
    {"asof": "<raphtory:asof:2022-01-01>", "staff": 1},
    {"asof": "<raphtory:asof:2023-01-01>", "staff": 2},
    {"asof": "<raphtory:asof:2024-01-01>", "staff": 1},
]
```

!!! Output

    ```output
    {'asof': '<raphtory:asof:2022-01-01>', 'staff': 1}
    {'asof': '<raphtory:asof:2023-01-01>', 'staff': 2}
    {'asof': '<raphtory:asof:2024-01-01>', 'staff': 1}
    ```

The IRI of a time graph is not the encoding of any Raphtory name (an encoded name never has a raw `:` after
`raphtory:`), so it is returned in its N-Triples form, with angle brackets, as described in
[Values are Raphtory names](2_sparql.md#values-are-raphtory-names). As usual with `GROUP BY`, a time at which nothing
matches has no row at all rather than a count of `0`.

Listing the times with `VALUES` gives the same result, and keeps the current state as the default graph (see the
[rules](#rules-for-time-graphs) below):

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?asof (COUNT(?person) AS ?staff) WHERE {
        VALUES ?asof { raphtory:asof:2022-01-01 raphtory:asof:2023-01-01 raphtory:asof:2024-01-01 }
        GRAPH ?asof { ?person ex:worksFor ex:acme }
    }
    GROUP BY ?asof ORDER BY ?asof
""", decode_literals=True)
```
///

```{.python continuation hide}
assert rows == [
    {"asof": "<raphtory:asof:2022-01-01>", "staff": 1},
    {"asof": "<raphtory:asof:2023-01-01>", "staff": 2},
    {"asof": "<raphtory:asof:2024-01-01>", "staff": 1},
]
```

To build the IRIs of the times from other values, bind them first and match them inside `LATERAL { ... }`, which runs
its pattern once for each binding:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?year (COUNT(?person) AS ?staff) WHERE {
        VALUES ?year { 2022 2023 2024 }
        BIND(IRI(CONCAT("raphtory:asof:", STR(?year), "-01-01")) AS ?asof)
        LATERAL { GRAPH ?asof { ?person ex:worksFor ex:acme } }
    }
    GROUP BY ?year ORDER BY ?year
""", decode_literals=True)
for row in rows:
    print(row)
```
///

```{.python continuation hide}
assert rows == [{"year": 2022, "staff": 1}, {"year": 2023, "staff": 2}, {"year": 2024, "staff": 1}]
```

!!! Output

    ```output
    {'year': 2022, 'staff': 1}
    {'year': 2023, 'staff': 2}
    {'year': 2024, 'staff': 1}
    ```

### Rules for time graphs

- **`FROM`** makes a time graph the default graph of the query: `SELECT ... FROM raphtory:asof:2023-01-01 WHERE { ... }`
  is the same as running the query on `g.snapshot_at("2023-01-01")`. With several `FROM` clauses, a triple that is
  visible at several of the times is matched once per time, so use `SELECT DISTINCT` or `COUNT(DISTINCT ...)`.
- **`GRAPH ?g`.** Time graphs cannot be listed, so `GRAPH ?g { ... }` with an unbound `?g` only visits the time graphs
  the query names: the ones of `FROM NAMED` or, without `FROM NAMED`, the ones it writes as constants anywhere, for
  example in `VALUES`, `BIND` or `FILTER`. A time graph IRI built from other values, such as
  `IRI(CONCAT("raphtory:asof:", ?date))`, is not a constant: put the `GRAPH ?g` pattern inside `LATERAL { ... }` after
  the `BIND`, as above. Elsewhere it may not be matched.
- **`FROM NAMED`.** As SPARQL specifies, once a query has a `FROM` or `FROM NAMED` clause, its `GRAPH` patterns only see
  the graphs named by `FROM NAMED`. Without a `FROM` clause, a query with `FROM NAMED` also has an empty default graph,
  so its patterns outside `GRAPH` match nothing. To keep the current state as the default graph, add
  `FROM raphtory:asof:T` with a time `T` after all events, such as `FROM raphtory:asof:9999-12-31`, or name the time
  graphs with `VALUES` or constant `GRAPH raphtory:asof:T` patterns instead of `FROM NAMED`.
- **Spelling.** Different spellings of the same time, such as `raphtory:asof:2024-01-01` and
  `raphtory:asof:1704067200000`, are different IRIs and so different graphs with the same triples.
- **Views.** Time graphs are taken from the view the query runs on, so they combine with its layers, subgraphs and
  filters. Time windows intersect: `GRAPH raphtory:asof:T` is the view's `snapshot_at(T)`, so on a time view of a
  `PersistentGraph` it is empty unless `T` is inside the view's window. There, `snapshot_at(t)`, `at(t)` and
  `snapshot_latest()` are windows of a single instant, so in them every time graph at another time is empty, even for
  triples that held at that time; `before(t)` keeps only the times before `t`, `after(t)` only the times after `t`, and
  `window(start, end)` only the times from `start` up to but excluding `end`. Run queries with time graphs on the graph
  itself, or on layer, subgraph and filter views, which have no time window.
- **Event graphs.** On an event `Graph`, `<raphtory:asof:T>` holds every triple asserted at or before `T`, like
  `g.snapshot_at(T)`. On a window of a `Graph`, a time before the start of the window gives an empty graph, and a time
  at or after its end gives every triple asserted in the window.

```{.python continuation hide}
import pytest
from raphtory import Graph

# every format of T is the same instant
instant = PersistentGraph()
instant.load_rdf(1704067200000, works_for("alice", "acme"))
for t in [
    "1704067200000",
    "2024-01-01",
    "2024-01-01T00:00:00",
    "2024-01-01T00:00:00.000",
    "2024-01-01T00:00:00Z",
    "<raphtory:asof:2024-01-01T01:00:00+01:00>",
]:
    graph = t if t.startswith("<") else "raphtory:asof:" + t
    assert instant.sparql("ASK { GRAPH " + graph + " { ?s ?p ?o } }") is True, t
assert instant.sparql("ASK { GRAPH raphtory:asof:1704067199999 { ?s ?p ?o } }") is False
with pytest.raises(Exception, match="invalid time graph"):
    instant.sparql("ASK { GRAPH raphtory:asof:yesterday { ?s ?p ?o } }")

# FROM is the snapshot; several FROM clauses repeat triples
staff = "SELECT ?person ?company WHERE { ?person <http://example.org/worksFor> ?company } ORDER BY ?person"
staff_from = "SELECT ?person ?company FROM raphtory:asof:2023-01-01 WHERE { ?person <http://example.org/worksFor> ?company } ORDER BY ?person"
assert g.sparql(staff_from) == g.snapshot_at("2023-01-01").sparql(staff)
two_froms = "SELECT MODIFIER ?person FROM raphtory:asof:2023-01-01 FROM raphtory:asof:2024-01-01 WHERE { ?person <http://example.org/worksFor> <http://example.org/acme> }"
assert len(g.sparql(two_froms.replace("MODIFIER", ""))) == 3
assert len(g.sparql(two_froms.replace("MODIFIER", "DISTINCT"))) == 2

# GRAPH ?g visits the time graphs the query names, wherever it is; a built IRI needs LATERAL
assert g.sparql("ASK { GRAPH ?g { ?s ?p ?o } }") is False
assert g.sparql("SELECT ?g WHERE { GRAPH ?g { } FILTER(?g = raphtory:asof:2022-01-01) }") == [{"g": "<raphtory:asof:2022-01-01>"}]
in_2022 = "VALUES ?g { raphtory:asof:2022-01-01 } GRAPH ?g { ?person ?p ?company OPTIONAL { ?company ?q ?r } }"
assert g.sparql("SELECT ?person WHERE { " + in_2022 + " }") == [{"person": "http://example.org/alice"}]
built = 'BIND(IRI(CONCAT("raphtory:asof:", "2022-01-01")) AS ?g)'
pattern = "GRAPH ?g { ?person ?p ?company OPTIONAL { ?company ?q ?r } }"
assert g.sparql("SELECT ?person WHERE { " + built + " " + pattern + " }") == []
assert g.sparql("SELECT ?person WHERE { " + built + " LATERAL { " + pattern + " } }") == [{"person": "http://example.org/alice"}]

# with FROM or FROM NAMED, GRAPH only sees the FROM NAMED graphs
assert g.sparql("ASK FROM <http://example.org/other> { GRAPH raphtory:asof:2023-01-01 { ?s ?p ?o } }") is False
assert g.sparql("ASK FROM NAMED raphtory:asof:2022-01-01 { GRAPH raphtory:asof:2023-01-01 { ?s ?p ?o } }") is False

# different spellings are different graphs with the same triples
spellings = g.sparql("""
    SELECT ?asof (COUNT(?person) AS ?staff)
    FROM NAMED raphtory:asof:2023-01-01
    FROM NAMED raphtory:asof:1672531200000
    WHERE { GRAPH ?asof { ?person <http://example.org/worksFor> <http://example.org/acme> } }
    GROUP BY ?asof ORDER BY ?asof
""", decode_literals=True)
assert spellings == [
    {"asof": "<raphtory:asof:1672531200000>", "staff": 2},
    {"asof": "<raphtory:asof:2023-01-01>", "staff": 2},
]

# time graphs follow the view: subgraphs, and windows intersect
assert g.sparql("ASK { GRAPH raphtory:asof:2023-01-01 { ?s ?p <http://example.org/acme> } }") is True
assert g.exclude_nodes(["http://example.org/acme"]).sparql("ASK { GRAPH raphtory:asof:2023-01-01 { ?s ?p ?o } }") is False
window = g.window("2022-01-01", "2023-01-01")
assert window.sparql("ASK { GRAPH raphtory:asof:2022-06-01 { ?s ?p ?o } }") is True
assert window.sparql("ASK { GRAPH raphtory:asof:2021-12-01 { ?s ?p ?o } }") is False
assert window.sparql("ASK { GRAPH raphtory:asof:2023-06-01 { ?s ?p ?o } }") is False

# on an event graph, asof:T is "asserted at or before T", and windows intersect
events = Graph()
events.load_rdf("2021-06-01", works_for("alice", "acme"))
events.retract_rdf("2023-06-01", works_for("alice", "acme"))
events.load_rdf("2023-06-01", works_for("alice", "initech"))
def in_graph(t):
    return "SELECT ?company WHERE { GRAPH raphtory:asof:" + t + " { ?person <http://example.org/worksFor> ?company } } ORDER BY ?company"


assert events.sparql(in_graph("2024-01-01")) == [{"company": "http://example.org/acme"}, {"company": "http://example.org/initech"}]
assert events.sparql(in_graph("2022-01-01")) == [{"company": "http://example.org/acme"}]
events_2023 = events.window("2023-01-01", "2024-01-01")
assert events_2023.sparql(in_graph("2022-01-01")) == []
assert events_2023.sparql(in_graph("2025-01-01")) == [{"company": "http://example.org/initech"}]

# on a PersistentGraph, snapshot_at, at and snapshot_latest are single instants, and before and after are windows:
# time graphs outside them are empty
asof_2022 = "ASK { GRAPH raphtory:asof:2022-01-01 { ?s ?p ?o } }"
assert g.sparql(asof_2022) is True
assert g.layer("http://example.org/worksFor").sparql(asof_2022) is True
assert g.snapshot_latest().sparql(asof_2022) is False
assert g.snapshot_at("2023-01-01").sparql(asof_2022) is False
assert g.at("2023-01-01").sparql(asof_2022) is False
assert g.snapshot_at("2022-01-01").sparql(asof_2022) is True
assert g.after("2022-01-01").sparql(asof_2022) is False
assert g.after("2021-12-01").sparql(asof_2022) is True
assert g.before("2022-01-01").sparql(asof_2022) is False
assert g.before("2022-01-02").sparql(asof_2022) is True
changed = """
    PREFIX ex: <http://example.org/>
    SELECT ?person WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ?before }
        GRAPH raphtory:asof:2024-01-01 { ?person ex:worksFor ?after }
        FILTER(?before != ?after)
    }
"""
assert g.sparql(changed) == [{"person": "http://example.org/alice"}]
assert g.snapshot_latest().sparql(changed) == []
assert g.snapshot_at("2025-01-01").sparql(changed) == []

# without FROM, a query with FROM NAMED has an empty default graph; FROM a time after all events keeps the current state
assert g.sparql("ASK { ?s ?p ?o }") is True
assert g.sparql("ASK FROM NAMED raphtory:asof:2022-01-01 { ?s ?p ?o }") is False
still_at_initech = """
    PREFIX ex: <http://example.org/>
    SELECT ?asof ?person DEFAULT_GRAPH
    FROM NAMED raphtory:asof:2022-01-01
    FROM NAMED raphtory:asof:2023-01-01
    WHERE { ?person ex:worksFor ex:initech . GRAPH ?asof { ?person ex:worksFor ex:acme } }
    ORDER BY ?asof
"""
assert g.sparql(still_at_initech.replace("DEFAULT_GRAPH", "")) == []
assert g.sparql(still_at_initech.replace("DEFAULT_GRAPH", "FROM raphtory:asof:9999-12-31")) == [
    {"asof": "<raphtory:asof:2022-01-01>", "person": "http://example.org/alice"},
    {"asof": "<raphtory:asof:2023-01-01>", "person": "http://example.org/alice"},
]
# VALUES and constant GRAPH patterns keep the default graph
assert g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?asof ?person WHERE {
        VALUES ?asof { raphtory:asof:2022-01-01 raphtory:asof:2023-01-01 }
        ?person ex:worksFor ex:initech . GRAPH ?asof { ?person ex:worksFor ex:acme }
    }
    ORDER BY ?asof
""") == [
    {"asof": "<raphtory:asof:2022-01-01>", "person": "http://example.org/alice"},
    {"asof": "<raphtory:asof:2023-01-01>", "person": "http://example.org/alice"},
]
assert g.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person WHERE {
        ?person ex:worksFor ex:initech .
        GRAPH raphtory:asof:2022-01-01 { ?person ex:worksFor ex:acme }
    }
""") == [{"person": "http://example.org/alice"}]

# the time views of a persistent graph
history = PersistentGraph()
history.load_rdf(1, works_for("alice", "acme"))
history.retract_rdf(5, works_for("alice", "acme"))
ask = "ASK { ?s ?p ?o }"
assert history.sparql(ask) is False
assert history.after(2).sparql(ask) is False
assert history.snapshot_latest().sparql(ask) is False
assert history.snapshot_at(4).sparql(ask) is True
assert history.before(5).sparql(ask) is True
assert history.before(6).sparql(ask) is False
assert history.window(3, 5).sparql(ask) is True
assert history.window(0, 6).sparql(ask) is False
assert history.snapshot_at(3).to_rdf() == works_for("alice", "acme").decode() + "\n"
```

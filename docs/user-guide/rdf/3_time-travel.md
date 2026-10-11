# Time travel

Every triple has a history: it is asserted at a time, can be retracted later and asserted again, and every event is
kept. A triple holds from an assertion until the next retraction. You can ask what was true at any time in two ways:

- pick a view of the `RdfGraph`, such as `rg.valid()` or `rg.at(t)`, and query, export or validate it;
- name times inside a query with the time graphs `<raphtory:asof:T>`, to compare several times in one query.

## A history of employment

Alice joins Acme in June 2021 and Bob joins in March 2022. In June 2023 Alice leaves Acme for Initech, recorded as the
retraction of one triple and the assertion of another at the same time:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


rg = RdfGraph()
rg.load_rdf("2021-06-01", works_for("alice", "acme"))
rg.load_rdf("2022-03-01", works_for("bob", "acme"))
rg.retract_rdf("2023-06-01", works_for("alice", "acme"))
rg.load_rdf("2023-06-01", works_for("alice", "initech"))
```
///

## Views pick the time

A query sees exactly the triples of the view it runs on:

| View                                                   | The triples it shows                                                                 |
|--------------------------------------------------------|--------------------------------------------------------------------------------------|
| `rg`                                                   | every triple ever asserted (or retracted)                                            |
| `rg.valid()`, `rg.latest()`                            | the current state                                                                    |
| `rg.at(t)`, `rg.snapshot_at(t)`                        | the state as of `t`                                                                  |
| `rg.window(start, end)`, `rg.before(t)`, `rg.after(t)` | every triple that held at some point in the window, or was retracted after its start |
| `view.valid()`                                         | the triples of `view` that hold at its end                                           |

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
query = """
    SELECT ?person ?company WHERE { ?person <http://example.org/worksFor> ?company }
    ORDER BY ?person ?company
"""
print("now         ", rg.valid().sparql(query))
print("2023-01-01  ", rg.at("2023-01-01").sparql(query))
print("during 2023 ", rg.window("2023-01-01", "2024-01-01").sparql(query))
print("ever        ", rg.sparql(query))
```
///

```{.python continuation hide}
alice_acme = {"person": "http://example.org/alice", "company": "http://example.org/acme"}
alice_initech = {"person": "http://example.org/alice", "company": "http://example.org/initech"}
bob_acme = {"person": "http://example.org/bob", "company": "http://example.org/acme"}
assert rg.valid().sparql(query) == [alice_initech, bob_acme]
assert rg.latest().sparql(query) == [alice_initech, bob_acme]
assert rg.at("2021-01-01").sparql(query) == []
assert rg.at("2022-01-01").sparql(query) == [alice_acme]
assert rg.snapshot_at("2023-01-01").sparql(query) == [alice_acme, bob_acme]
assert rg.at("2023-06-01").sparql(query) == [alice_initech, bob_acme]
assert rg.window("2023-01-01", "2024-01-01").valid().sparql(query) == [alice_initech, bob_acme]
assert rg.sparql(query) == [alice_acme, alice_initech, bob_acme]
```

!!! Output

    ```output
    now          [{'person': 'http://example.org/alice', 'company': 'http://example.org/initech'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    2023-01-01   [{'person': 'http://example.org/alice', 'company': 'http://example.org/acme'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    during 2023  [{'person': 'http://example.org/alice', 'company': 'http://example.org/acme'}, {'person': 'http://example.org/alice', 'company': 'http://example.org/initech'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    ever         [{'person': 'http://example.org/alice', 'company': 'http://example.org/acme'}, {'person': 'http://example.org/alice', 'company': 'http://example.org/initech'}, {'person': 'http://example.org/bob', 'company': 'http://example.org/acme'}]
    ```

The same views work with `to_rdf()` and `validate_shacl()`, so `rg.at("2023-01-01").to_rdf()` exports the state at the
start of 2023.

### The order of views

`valid()` applies at the end of the view it is called on, so the order matters: `x.valid()` is the state at the end of
`x`, and `valid().x` is the current state further restricted to `x`. With `p` valid from 10 to 30 and again from 50,
`q` from 5, `r` from 12 to 20 and `s` from 22 to 40:

| View               | Alone      | Then `.valid()` | `.valid()` first, then the view |
|--------------------|------------|-----------------|---------------------------------|
| `at(25)`           | p, q, s    | p, q, s         | p, q                            |
| `at(35)`           | q, s       | q, s            | q                               |
| `window(15, 35)`   | p, q, r, s | q, s            | p, q                            |
| `before(25)`       | p, q, r, s | p, q, s         | p, q                            |

```{.python hide}
from raphtory import RdfGraph

rg = RdfGraph()
for time, write, triple in [
    (10, "load", "p"), (30, "retract", "p"), (50, "load", "p"), (5, "load", "q"),
    (12, "load", "r"), (20, "retract", "r"), (22, "load", "s"), (40, "retract", "s"),
]:
    doc = f"<http://e/a> <http://e/{triple}> <http://e/b> .".encode()
    (rg.load_rdf if write == "load" else rg.retract_rdf)(time, doc)


def shown(view):
    return [row["p"].removeprefix("http://e/") for row in view.sparql("SELECT ?p WHERE { ?s ?p ?o } ORDER BY ?p")]


assert shown(rg.at(25)) == shown(rg.at(25).valid()) == ["p", "q", "s"]
assert shown(rg.valid().at(25)) == ["p", "q"]
assert shown(rg.at(35)) == shown(rg.at(35).valid()) == ["q", "s"]
assert shown(rg.valid().at(35)) == ["q"]
assert shown(rg.window(15, 35)) == ["p", "q", "r", "s"]
assert shown(rg.window(15, 35).valid()) == ["q", "s"]
assert shown(rg.valid().window(15, 35)) == ["p", "q"]
assert shown(rg.before(25)) == ["p", "q", "r", "s"]
assert shown(rg.before(25).valid()) == ["p", "q", "s"]
assert shown(rg.valid().before(25)) == ["p", "q"]
```

## How assertions and retractions combine

- Assertions and retractions are ordered by time, not by the order in which they were written.
- If an assertion and a retraction of the same triple have the same time, the one written last wins.
- Asserting a triple that already holds changes nothing visible, but the event is kept. One retraction ends it.
- Retractions are always recorded, even for a triple that was never asserted. The subject and object nodes are
  created, and the whole graph and any window containing the retraction show the triple, but `valid()` and `at(t)` do
  not.
- Retracting a triple does not affect other triples between the same two nodes, because each predicate is a separate
  layer.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

triple = b"<http://example.org/a> <http://example.org/p> <http://example.org/b> ."
ask = "ASK { <http://example.org/a> <http://example.org/p> <http://example.org/b> }"

# written out of order: the retraction at time 5 is written before the assertion at time 1
out_of_order = RdfGraph()
out_of_order.retract_rdf(5, triple)
out_of_order.load_rdf(1, triple)
print([out_of_order.at(t).sparql(ask) for t in range(7)])

# the same time: the write made last wins
same_time = RdfGraph()
same_time.load_rdf(1, triple)
same_time.retract_rdf(1, triple)
print(same_time.at(1).sparql(ask))
```
///

!!! Output

    ```output
    [False, True, True, True, True, False, False]
    False
    ```

### The history of a triple

A triple is an edge in a layer, so the edge API of [graph][raphtory.RdfGraph.graph] gives its full history: the
[history][raphtory.Edge.history] holds the times it was asserted and the [deletions][raphtory.Edge.deletions] the times
it was retracted:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
from raphtory import RdfGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


rg = RdfGraph()
rg.load_rdf("2021-06-01", works_for("alice", "acme"))
rg.retract_rdf("2023-06-01", works_for("alice", "acme"))

edge = rg.graph.layer("http%3A%2F%2Fexample.org%2FworksFor").edge("http://example.org/alice", "http://example.org/acme")
print(edge.history.dt.collect())
print(edge.deletions.dt.collect())
```
///

!!! Output

    ```output
    [datetime.datetime(2021, 6, 1, 0, 0, tzinfo=datetime.timezone.utc)]
    [datetime.datetime(2023, 6, 1, 0, 0, tzinfo=datetime.timezone.utc)]
    ```

## Since when? Validity functions

Four functions answer "since when has this triple held, and until when?" inside a query:

| Function                          | Returns                                                                         |
|-----------------------------------|---------------------------------------------------------------------------------|
| `raphtory:validFrom(s, p, o)`     | the time since which the triple has held, as an `xsd:dateTime` in UTC            |
| `raphtory:validTo(s, p, o)`       | the time it stopped holding (the retraction that ended it), as an `xsd:dateTime` |
| `raphtory:validFromTime(s, p, o)` | the same time as `validFrom`, as a Raphtory time (milliseconds since the epoch)  |
| `raphtory:validToTime(s, p, o)`   | the same time as `validTo`, as a Raphtory time                                   |

Without a fourth argument they answer for the end of the view, so `validTo` is always unbound and `validFrom` is
unbound for a triple that does not hold there. This query lists the current jobs of the
[history above](#a-history-of-employment), with their start dates:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph


def works_for(person, company):
    return f"<http://example.org/{person}> <http://example.org/worksFor> <http://example.org/{company}> .".encode()


rg = RdfGraph()
rg.load_rdf("2021-06-01", works_for("alice", "acme"))
rg.load_rdf("2022-03-01", works_for("bob", "acme"))
rg.retract_rdf("2023-06-01", works_for("alice", "acme"))
rg.load_rdf("2023-06-01", works_for("alice", "initech"))

rows = rg.valid().sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person ?company ?since WHERE {
        ?person ex:worksFor ?company
        BIND(raphtory:validFrom(?person, ex:worksFor, ?company) AS ?since)
    }
    ORDER BY ?since
""", decode_literals=True)
for row in rows:
    print(row["person"], row["company"], row["since"].date())
```
///

!!! Output

    ```output
    http://example.org/bob http://example.org/acme 2022-03-01
    http://example.org/alice http://example.org/initech 2023-06-01
    ```

To ask about another time, pass a *reference time* as the fourth argument: a [time graph](#comparing-times-in-one-query)
such as `raphtory:asof:2023-01-01`, an integer Raphtory time or an `xsd:dateTime` (UTC if it has no timezone). The
functions then return the interval of validity that contains it:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = rg.sparql("""
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

!!! Output

    ```output
    http://example.org/alice 2021-06-01 2023-06-01
    http://example.org/bob 2022-03-01 None
    ```

Functions cannot see the `GRAPH` or `FROM` they are called in. To follow the time graphs of a `GRAPH ?g` pattern, pass
`?g` as the reference time, in a `BIND` after the `GRAPH` pattern (inside it, `?g` is not bound yet):

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = rg.sparql("""
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

!!! Output

    ```output
    <raphtory:asof:2022-01-01> http://example.org/alice 2023-06-01
    <raphtory:asof:2023-01-01> http://example.org/alice 2023-06-01
    <raphtory:asof:2023-01-01> http://example.org/bob None
    ```

The rules:

- **Only visible triples.** The functions are unbound for a triple the view does not show (as of the reference time,
  if there is one), so they never reveal triples hidden by layers, subgraphs, filters or windows.
- **The whole history counts, up to the end of the view.** On `rg.window(start, end)` an interval can begin before
  `start`; on `rg.at(t)` the events after `t` are ignored.
- **Literal objects** are matched by their canonical form, because SPARQL passes numbers, booleans and dates to
  functions in canonical form: `42` finds `"042"^^xsd:integer`, but not `42.0`. Pass the object bound by the pattern.
  Several objects with the same canonical form give an unbound result.
- **No errors.** A wrong number of arguments, an unknown term or a reference that is not a time gives an unbound
  result. A misspelled function name, such as `raphtory:validfrom`, makes the query fail.
- **Dates.** `validFrom` and `validTo` are unbound outside the years 1 to 9999; `validFromTime` and `validToTime` still
  answer.

## Comparing times in one query

The time graphs `<raphtory:asof:T>` are virtual, read-only named graphs: `GRAPH <raphtory:asof:T> { ... }` matches its
patterns against `snapshot_at(T)` of the view the query runs on. On `rg` that is the state as of `T`. `T` is epoch
milliseconds or a date-time (UTC unless it has a timezone):

| Format                                      | Example                                       |
|---------------------------------------------|-----------------------------------------------|
| milliseconds since the Unix epoch           | `raphtory:asof:1704067200000`                 |
| a date (midnight UTC)                       | `raphtory:asof:2024-01-01`                    |
| a date and time                             | `raphtory:asof:2024-01-01T00:00:00`, `raphtory:asof:2024-01-01T00:00:00.000` |
| an RFC 3339 date and time with a timezone   | `raphtory:asof:2024-01-01T00:00:00Z`, `<raphtory:asof:2024-01-01T01:00:00+01:00>` |

A `+` cannot appear in a prefixed name, so write a `+` offset as a full IRI. A time that does not parse fails the query
with `invalid time graph`.

This query finds who changed employer between the start of 2023 and the start of 2024:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = rg.sparql("""
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

!!! Output

    ```output
    [{'person': 'http://example.org/alice', 'before': 'http://example.org/acme', 'after': 'http://example.org/initech'}]
    ```

To compare a past time with now in one query, keep the query on `rg` and filter the current triples with `validFrom`,
which is bound only for a triple that holds now:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(rg.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?person ?then ?now WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ?then }
        ?person ex:worksFor ?now
        BIND(raphtory:validFrom(?person, ex:worksFor, ?now) AS ?since)
        FILTER(BOUND(?since) && ?then != ?now)
    }
"""))
```
///

!!! Output

    ```output
    [{'person': 'http://example.org/alice', 'then': 'http://example.org/acme', 'now': 'http://example.org/initech'}]
    ```

To visit several times with one pattern, name them with `FROM NAMED` or `VALUES` and match them with `GRAPH ?g`:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = rg.sparql("""
    PREFIX ex: <http://example.org/>
    SELECT ?asof (COUNT(?person) AS ?staff) WHERE {
        VALUES ?asof { raphtory:asof:2022-01-01 raphtory:asof:2023-01-01 raphtory:asof:2024-01-01 }
        GRAPH ?asof { ?person ex:worksFor ex:acme }
    }
    GROUP BY ?asof ORDER BY ?asof
""", decode_literals=True)
for row in rows:
    print(row)
```
///

!!! Output

    ```output
    {'asof': '<raphtory:asof:2022-01-01>', 'staff': 1}
    {'asof': '<raphtory:asof:2023-01-01>', 'staff': 2}
    {'asof': '<raphtory:asof:2024-01-01>', 'staff': 1}
    ```

### Rules for time graphs

- **Run them on the whole graph.** A time graph is `snapshot_at(T)` of the view, so every filter of the view applies:
  on `rg.valid()` it holds only the triples that held at `T` and still hold now, and on a window it is empty unless
  `T` is inside the window. Layer, subgraph and node or edge filter views keep time graphs working.
- **`FROM`** makes a time graph the default graph: `SELECT ... FROM raphtory:asof:2023-01-01 WHERE { ... }` on `rg`
  is the query on `rg.at("2023-01-01")`. Several `FROM` clauses are concatenated, so a triple visible at several times
  is matched once per time: use `DISTINCT`.
- **`GRAPH ?g`.** Time graphs cannot be listed: `GRAPH ?g { ... }` with an unbound `?g` visits the `FROM NAMED` graphs
  or, without `FROM NAMED`, the time graphs the query writes as constants (in `VALUES`, `BIND`, `FILTER`, ...). An IRI
  built from other values, such as `IRI(CONCAT("raphtory:asof:", ?date))`, is only matched by a `GRAPH ?g` inside
  `LATERAL { ... }` after the `BIND`.
- **`FROM NAMED`.** As SPARQL specifies, a query with `FROM NAMED` and no `FROM` has an empty default graph. Name the
  times with `VALUES` or constant `GRAPH` patterns to keep the default graph.
- **Spelling.** `raphtory:asof:2024-01-01` and `raphtory:asof:1704067200000` are different graphs with the same triples.

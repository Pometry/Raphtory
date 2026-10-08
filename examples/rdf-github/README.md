# A GitHub repository as a temporal knowledge graph

This example loads the history of a GitHub repository (its pull requests, reviews and issues) into Raphtory as RDF
triples, each written at the time it happened. You can then query the repository as it is now or as it was on any
date, with SPARQL from a query editor in the browser, and validate it with SHACL.

It also builds a small graph of people and the companies they work for, which needs no download.

## Requirements

- Python with `raphtory` built from this repository, which includes RDF support. Run `make build-python` at the
  repository root, then `make check` here to confirm.
- The [GitHub CLI](https://cli.github.com/), logged in (`gh auth login`), to download a repository's history. The
  people graph does not need it.
- A browser with internet access: the query pages load [YASGUI](https://github.com/Matdata-eu/Yasgui) and its
  [graph plugin](https://github.com/Matdata-eu/yasgui-graph-plugin) from jsdelivr.

## Quick start

```bash
make serve
```

This downloads the history of `pometry/raphtory` (a few minutes, the first time only), builds both graphs and starts:

- the Raphtory server on <http://localhost:1736>, with its UI, GraphQL and the SPARQL endpoints
  `http://localhost:1736/sparql/github` and `http://localhost:1736/sparql/people`;
- the query pages on <http://localhost:1737>: YASGUI with a tab per example query, each shown as a table or a graph.

Press Ctrl+C to stop both.

| Target           | What it does                                                                         |
|------------------|--------------------------------------------------------------------------------------|
| `make serve`     | builds whatever is missing, then runs the server and the query pages                 |
| `make fetch`     | downloads the history of `REPO` into `data/` again                                   |
| `make graphs`    | builds `graphs/github` and `graphs/people` (`make github`, `make people` build one)  |
| `make validate`  | checks the github graph against [`schema/github.ttl`](schema/github.ttl) over time   |
| `make check`     | checks that the installed `raphtory` has RDF support                                 |
| `make clean`     | removes the graphs (`make distclean` also removes the downloaded data)               |

Variables: `REPO=owner/name` picks another repository (run `make distclean` first if you have already downloaded one),
`PORT` and `UI_PORT` move the two servers, and `PYTHON` picks the Python interpreter.

## The graph

Every triple is an edge from its subject to its object, on a layer named after its predicate. The IRIs of pull
requests, issues, labels and developers are their GitHub URLs, such as `https://github.com/Pometry/Raphtory/pull/2783`
and `https://github.com/miratepuffin`. The vocabulary is under `http://example.org/gh/` (`gh:`).

| Subject             | Predicate     | Object                                  | Asserted when                       | Retracted when                 |
|---------------------|---------------|-----------------------------------------|-------------------------------------|--------------------------------|
| pull request        | `rdf:type`    | `gh:PullRequest`                        | it is opened                        |                                |
| pull request        | `gh:title`    | string                                  | it is opened                        |                                |
| pull request        | `gh:author`   | developer                               | it is opened                        |                                |
| pull request        | `gh:state`    | `gh:Open`                               | it is opened                        | it is merged or closed         |
| pull request        | `gh:state`    | `gh:Merged` or `gh:Closed`              | it is merged or closed              |                                |
| pull request        | `gh:mergedBy` | developer                               | it is merged                        |                                |
| pull request        | `gh:additions`, `gh:deletions` | `xsd:integer`, lines in its final diff | it is opened          |                                |
| pull request        | `gh:label`    | label                                   | it is opened                        |                                |
| pull request        | `gh:closes`   | issue (possibly of another repository)  | it is opened                        |                                |
| developer           | `gh:reviewed` | pull request (not their own)            | the review is submitted             |                                |
| developer           | `gh:approved` | pull request                            | an approving review is submitted    |                                |
| issue               | `rdf:type`    | `gh:Issue`                              | it is opened                        |                                |
| issue               | `gh:title`    | string                                  | it is opened                        |                                |
| issue               | `gh:author`   | developer                               | it is opened                        |                                |
| issue               | `gh:state`    | `gh:Open`                               | it is opened                        | it is closed                   |
| issue               | `gh:state`    | `gh:Closed`                             | it is closed                        |                                |
| issue               | `gh:label`    | label                                   | it is opened                        |                                |
| issue               | `gh:assignee` | developer                               | it is opened                        |                                |
| developer, label    | `rdf:type`    | `gh:Developer`, `gh:Label`              | it first appears                    |                                |
| developer, label    | `rdfs:label`  | string: the login or the label's name   | it first appears                    |                                |

A few things follow from writing every event at its own time:

- **The state of anything at any time.** A pull request holds exactly one `gh:state` at a time, because merging or
  closing it retracts `gh:Open`. Querying the graph as of a date gives the pull requests open on that date.
- **When something happened** is when its triple was asserted: `raphtory:validFrom(?pr, gh:state, gh:Merged)` is the
  time a pull request was merged, and `raphtory:validFrom(?pr, gh:author, ?dev)` the time it was opened.
- **Values are nodes.** Titles, logins and line counts are literal nodes, so values shared by several subjects join
  them in the graph.

The data does not record when labels or assignees changed, or when issues were reopened: labels and assignees are
asserted when the pull request or issue is opened, and an issue's state reflects only its last closing.

## Querying

The query pages are the quickest way in; each tab says what it shows. Any SPARQL client can use the endpoints.
To query the past:

- add `?asof=2024-06-01` to an endpoint URL, which runs every query on the graph as it was on that date
  (in YASGUI, put it in the endpoint field);
- or name a time in the query: `FROM <raphtory:asof:2024-06-01>` for the whole query, or
  `GRAPH <raphtory:asof:2024-06-01> { ... }` for part of it, to compare dates in one query.

```bash
curl -H 'Accept: text/csv' \
  --data-urlencode 'query=PREFIX gh: <http://example.org/gh/>
    SELECT ?pr ?title WHERE { ?pr gh:state gh:Open ; gh:title ?title }' \
  'http://localhost:1736/sparql/github?asof=2024-06-01'
```

From Python, without a server:

```python
from raphtory import PersistentGraph

g = PersistentGraph.load_from_file("graphs/github")
rows = g.snapshot_at("2024-06-01").sparql("""
    PREFIX gh: <http://example.org/gh/>
    SELECT (COUNT(*) AS ?open) WHERE { ?pr a gh:PullRequest ; gh:state gh:Open }
""", decode_literals=True)
print(rows)  # [{'open': 6}] for pometry/raphtory
```

The Raphtory UI on <http://localhost:1736> shows the same graph as nodes and edges, and the GraphQL API has the
same queries as `graph(path: "github") { sparql(query: "...") }`.

## Validation

[`schema/github.ttl`](schema/github.ttl) describes the graph as SHACL shapes. `make validate` checks the graph against
them now, as of the start of each year of its history, and on its event view, which keeps every triple ever asserted
and ignores retractions:

```output
now:          conforms
as of 2018-01-01: conforms
as of 2019-01-01: conforms
...
as of 2026-01-01: conforms
event view:   2575 violations (2575 x http://example.org/gh/state MaxCountConstraintComponent)
```

The graph conforms at every time, but its event view does not: there, a merged pull request is both open and merged.

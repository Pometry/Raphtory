# Validating with SHACL

[SHACL](https://www.w3.org/TR/shacl/) is the W3C language for describing what valid RDF data looks like: a *shapes
graph* says, for example, that every `ex:Person` has exactly one `ex:name`, or that an age is a non-negative integer.
[validate_shacl()][raphtory.GraphView.validate_shacl] checks the RDF triples of any graph or view against a shapes graph
and returns a *validation report*: whether the data conforms, and one result per violation.

Validation sees the same triples as [sparql()][raphtory.GraphView.sparql] and `to_rdf()`: one triple per visible edge
and layer (see the [introduction](1_intro.md#how-triples-become-a-graph)). So, as with SPARQL, time travel is picking
the view: `g.snapshot_at(t).validate_shacl(shapes)` validates the data as it was at time `t`, and
[validate_shacl_at()][raphtory.GraphView.validate_shacl_at] validates several times at once.

!!! info

    SHACL validation is part of the `raphtory` Python package. In Rust it is behind the `shacl` cargo feature of the
    `raphtory` crate (which turns on `rdf`), in the
    [`raphtory::rdf::shacl`](https://docs.rs/raphtory/latest/raphtory/rdf/shacl/index.html) module. Validation runs
    natively, with the SHACL engine of [rudof](https://rudof-project.github.io/), on the triples of the graph as it
    reads them, without copying the graph.

## Validating a graph

The examples on this page use a small graph of people. Alice loses her name at time 5, and Bob gets a negative age at
time 7:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()
g.load_rdf(1, b"""
    @prefix ex: <http://example.org/> .
    ex:alice a ex:Person ; ex:name "Alice" ; ex:age 42 .
    ex:bob   a ex:Person ; ex:name "Bob" .
""")
g.retract_rdf(5, b'<http://example.org/alice> <http://example.org/name> "Alice" .')
g.load_rdf(7, b"<http://example.org/bob> <http://example.org/age> -3 .")

shapes = b"""
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex:  <http://example.org/> .

    ex:PersonShape a sh:NodeShape ;
        sh:targetClass ex:Person ;
        sh:property ex:PersonName , ex:PersonAge .
    ex:PersonName sh:path ex:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string .
    ex:PersonAge  sh:path ex:age ; sh:maxCount 1 ; sh:datatype xsd:integer ; sh:minInclusive 0 .
"""

report = g.validate_shacl(shapes)
print(report["conforms"])
for result in report["results"]:
    print(result["focus_node"], result["path"], result["constraint_component"], result["value"])
```
///

/// tab | :fontawesome-brands-rust: Rust
```rust
use raphtory::{
    prelude::*,
    rdf::{shacl::ShaclShapes, RdfFormat},
};

let g = PersistentGraph::new();
let people = r#"
    @prefix ex: <http://example.org/> .
    ex:alice a ex:Person ; ex:name "Alice" ; ex:age 42 .
    ex:bob   a ex:Person ; ex:name "Bob" .
"#;
g.load_rdf(1, people.as_bytes(), RdfFormat::Turtle, None).unwrap();
let alice_name = r#"<http://example.org/alice> <http://example.org/name> "Alice" ."#;
g.retract_rdf(5, alice_name.as_bytes(), RdfFormat::NTriples, None).unwrap();
let bob_age = "<http://example.org/bob> <http://example.org/age> -3 .";
g.load_rdf(7, bob_age.as_bytes(), RdfFormat::Turtle, None).unwrap();

let shapes_ttl = r#"
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex:  <http://example.org/> .

    ex:PersonShape a sh:NodeShape ;
        sh:targetClass ex:Person ;
        sh:property ex:PersonName , ex:PersonAge .
    ex:PersonName sh:path ex:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string .
    ex:PersonAge  sh:path ex:age ; sh:maxCount 1 ; sh:datatype xsd:integer ; sh:minInclusive 0 .
"#;
let shapes = ShaclShapes::parse(shapes_ttl.as_bytes(), RdfFormat::Turtle, None).unwrap();
let report = g.validate_shacl(&shapes).unwrap();
println!("{}", report.conforms);
for result in &report.results {
    // RDF terms: use `raphtory::rdf::name_of` to get the name of a node
    println!(
        "{} {:?} {} {:?}",
        result.focus_node,
        result.path.as_ref().map(|path| path.to_string()),
        result.constraint_component,
        result.value.as_ref().map(|value| value.to_string()),
    );
}
```
///

```{.python continuation hide}
assert not report["conforms"]
assert len(report["results"]) == 2
```

!!! Output

    ```output
    False
    http://example.org/alice http://example.org/name http://www.w3.org/ns/shacl#MinCountConstraintComponent None
    http://example.org/bob http://example.org/age http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent "-3"^^<http://www.w3.org/2001/XMLSchema#integer>
    ```

The shapes are an RDF document, given as `bytes` or as the path of a file. Turtle is read by default; pass `format` for
another RDF format (such as `"nt"`, `"jsonld"` or `"rdf"`), and `base_iri` to resolve relative IRIs.

The report is a dictionary with three keys:

- `conforms`: `True` if there are no results, of any severity.
- `results`: one dictionary per result, in a deterministic order, with
    - `focus_node`: the node that does not conform,
    - `path`: the path of the property shape, if the result comes from one: the predicate (a layer name) for a single
      predicate, otherwise a SPARQL property path such as `(<http://example.org/worksFor> / <http://example.org/name>)`,
    - `value`: the value that violates the constraint, if there is one (a count, such as `sh:minCount`, has none),
    - `source_shape`: the shape whose constraint is violated (a blank node of the shapes is `_:` followed by a label,
      which is the same each time the same shapes are validated),
    - `constraint_component`: the SHACL constraint component, such as `sh:MinCountConstraintComponent`,
    - `severity`: `sh:Violation`, unless the shape sets another `sh:severity`,
    - `messages`: the messages of the shape (`sh:message`) or, without one, a message of the validator.
- `warnings`: reasons why the results may not be those of the SHACL specification (see
  [subclasses](#subclasses) and [recursive shapes](#recursive-shapes)).

As with `sparql()`, terms are returned as Raphtory names, so a focus node or a value can be passed straight to
`node()`:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
alice = g.node(report["results"][0]["focus_node"])
print(alice.name)
```
///

!!! Output

    ```output
    http://example.org/alice
    ```

To get the report as a standard `sh:ValidationReport` RDF document instead, pass `report_format` with an RDF format
such as `"ttl"`, `"nt"` or `"jsonld"`. The document holds RDF terms, as `to_rdf()` writes them, and can be read by any
RDF tool. RDF/XML (`"rdf"`) cannot hold a literal with a control character other than tab and line feed (including a
carriage return), so a report whose focus node, value or message is such a literal raises an error in RDF/XML instead
of leaving out the triple (a result without its `sh:focusNode` is not a valid report). Use Turtle, N-Triples or JSON-LD
when the data can hold such literals:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
document = g.validate_shacl(shapes, report_format="ttl")
```
///

```{.python continuation hide}
assert "http://www.w3.org/ns/shacl#ValidationReport" in document
assert document.count("http://www.w3.org/ns/shacl#ValidationResult") == 2
```

## Validating the past

The report above describes the current state of the graph. On a `PersistentGraph`, `snapshot_at(t)` is the state as of
`t`, so it can be validated in the same way, and
[validate_shacl_at()][raphtory.GraphView.validate_shacl_at] validates several times, parsing the shapes once. It
returns each time with its report:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(g.snapshot_at(3).validate_shacl(shapes)["conforms"])

for t, report in g.validate_shacl_at(shapes, [1, 5, 7]):
    print(t, report["conforms"], len(report["results"]))

# when did the data first stop conforming?
reports = g.validate_shacl_at(shapes, list(range(10)))
print(next(t for t, report in reports if not report["conforms"]))
```
///

/// tab | :fontawesome-brands-rust: Rust
```rust
assert!(g.snapshot_at(3).validate_shacl(&shapes).unwrap().conforms);

for (t, report) in shapes.validate_at(&g, [1, 5, 7]).unwrap() {
    println!("{t} {} {}", report.conforms, report.results.len());
}
```
///

```{.python continuation hide}
assert g.snapshot_at(3).validate_shacl(shapes)["conforms"]
```

!!! Output

    ```output
    True
    1 True 0
    5 False 1
    7 False 2
    5
    ```

Times are anything a time can be in Raphtory, such as integers or date-time strings (`"2024-01-01"`), and are returned
as milliseconds. On an event `Graph`, a snapshot holds every triple asserted up to its time, and retractions are
ignored; use `g.persistent_graph()` to validate the state as of each time.

Every other view works too. For example, a [layer view](../views/3_layer.md) validates only some predicates, so the
names below are not seen and both people miss one:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
view = g.snapshot_at(3).exclude_layer("http://example.org/name")
for result in view.validate_shacl(shapes)["results"]:
    print(result["focus_node"], result["path"])
```
///

!!! Output

    ```output
    http://example.org/alice http://example.org/name
    http://example.org/bob http://example.org/name
    ```

## Graphs not loaded from RDF

Any graph can be validated. Names that are not RDF terms are IRIs under `raphtory:`, as in SPARQL: the node `Alice` is
`raphtory:Alice` and the layer `manages` is `raphtory:manages`, which shapes can write after declaring the prefix.
Results come back as names:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
g.add_edge(1, "Alice", "Bob", layer="manages")
g.add_edge(2, "Alice", "Carol", layer="manages")
g.add_edge(2, "Dave", "Erin", layer="manages")

# everyone who manages anyone manages at most one person
shapes = b"""
    @prefix sh:       <http://www.w3.org/ns/shacl#> .
    @prefix raphtory: <raphtory:> .
    <http://example.org/OneReport> sh:targetSubjectsOf raphtory:manages ;
        sh:property [ sh:path raphtory:manages ; sh:maxCount 1 ] .
"""
for result in g.validate_shacl(shapes)["results"]:
    print(result["focus_node"], result["path"])
print(g.window(0, 2).validate_shacl(shapes)["conforms"])
```
///

!!! Output

    ```output
    Alice manages
    True
    ```

## Supported SHACL

All of [SHACL Core](https://www.w3.org/TR/shacl/#core-components) is supported: every constraint component (value
types, cardinality, value ranges, strings, property pairs, logical constraints, shape-based constraints such as
`sh:node` and `sh:qualifiedValueShape`, `sh:closed`, `sh:hasValue` and `sh:in`), every property path, the targets
`sh:targetNode`, `sh:targetClass`, `sh:targetSubjectsOf`, `sh:targetObjectsOf` and implicit class targets,
`sh:deactivated`, `sh:severity` and `sh:message`, and [recursive shapes](#recursive-shapes). Raphtory runs the W3C
SHACL Core test suite on Raphtory graphs, and passes all 98 tests.

The W3C test suite is the git submodule `raphtory-rdf-tests/test-suites/data-shapes` of the Raphtory repository, a
checkout of [w3c/data-shapes](https://github.com/w3c/data-shapes) pinned to commit
`b923e580aaaccda57972a302754393d612de35fb`. Fetch it with
`git submodule update --init --checkout raphtory-rdf-tests/test-suites/data-shapes` (or `make w3c-tests-init`, which
also fetches the [SPARQL and Turtle test suites](4_limitations.md#conformance)), and `make rust-test-shacl-w3c` runs the
conformance tests on it: all 98 SHACL Core tests pass, the 22 SHACL-SPARQL tests are rejected as unsupported (15) or
expect a failure (7), and the SHACL 1.2 `sh:singleLine` test is rejected as unsupported. Without the submodule, the
tests print how to fetch it and pass. The `RAPHTORY_SHACL_TESTS` environment variable points them at another checkout.

Shapes that use a feature the validator does not evaluate, or would misread, fail to parse with an error that names
the feature (`unsupported SHACL feature: ...`), instead of being silently ignored:

- SHACL-SPARQL: `sh:sparql`, `sh:select`, `sh:ask`, `sh:SPARQLConstraint`, `sh:ConstraintComponent` and its
  validators, `sh:SPARQLTarget`, `sh:SPARQLFunction`, ...
- SHACL Advanced Features: `sh:rule`, `sh:TripleRule`, `sh:SPARQLRule`, `sh:target`, `sh:expression`, `sh:values`,
  `sh:filterShape`, ...
- SHACL-JS, and `sh:entailment` (validation sees the stored triples only, without inference).
- SHACL 1.2 features, such as `sh:singleLine`, `sh:minListLength`, `sh:memberShape`, `sh:rootClass`,
  `sh:targetWhere`, `sh:closed sh:ByTypes` and `sh:ShapeClass` (reifier shapes are read, but see below). Other
  unknown terms of the `sh:` namespace are ignored, as SHACL requires, so a misspelt term is ignored too.
- The inverse of a path that is not a predicate, such as `[ sh:inversePath ( ex:worksFor ex:name ) ]`. Write it with
  inverses of predicates instead: `^(p / q)` is `(^q / ^p)`, that is `( [ sh:inversePath ex:name ]
  [ sh:inversePath ex:worksFor ] )`; `^(p | q)` is `(^p | ^q)`, and `^(p*)` is `(^p)*`.
- A literal of `sh:hasValue` or `sh:in` that the validator would rewrite (see
  [literal values](#literal-values)): it compares the rewritten literal with the data, so it would never match.

The validator reads shapes, paths and RDF lists recursively, and a recursion too deep for its stack would end the
process. So shapes fail with `SHACL error: ...` if an RDF list (such as the values of `sh:in`) has more than 10,000
members, if shapes and paths nest more than 256 deep, or if a path or list contains itself. To check that values
belong to a longer list, use `sh:pattern`, or a SPARQL query.

`owl:imports` and `sh:shapesGraph` are not followed: the shapes graph is the document you pass. SHACL validation is not
available through the [GraphQL server](5_graphql.md): unlike a SPARQL query, a validation cannot be stopped by a
timeout, so the server cannot bound the work an untrusted request asks for. Validate in Python or Rust, or query the
server with SPARQL.

## Subclasses

SHACL asks validators to follow `rdfs:subClassOf` triples of the data: `sh:targetClass ex:Person` also targets the
instances of every subclass of `ex:Person`, and `sh:class ex:Person` accepts them. The validator does this only
partly:

1. `sh:targetClass` targets only the nodes whose `rdf:type` is the class itself, not those of a subclass.
2. An implicit class target (a shape that is also an `rdfs:Class`) follows a single `rdfs:subClassOf` step.
3. `sh:class` accepts the class and its direct subclasses, so a value two steps down (a `PhD` that is a `Student` that is
   a `Person`) is reported.

With data without subclass hierarchies, the results are not affected. When the shapes use `sh:targetClass`, `sh:class`
or implicit class targets and the view has an `rdfs:subClassOf` triple, the report has a warning:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()
g.load_rdf(1, b"""
    @prefix ex:   <http://example.org/> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    ex:Student rdfs:subClassOf ex:Person .
    ex:bob a ex:Student .
""")
shapes = b"""
    @prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix ex: <http://example.org/> .
    ex:PersonShape sh:targetClass ex:Person ; sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""
report = g.validate_shacl(shapes)
print(report["conforms"])
print(report["warnings"][0][:60])
```
///

!!! Output

    ```output
    True
    the data has rdfs:subClassOf triples, but the validator foll
    ```

Bob is a student, so a person without a name, but `sh:targetClass ex:Person` does not reach him. To validate such data,
add the types it implies before validating (for example with a SPARQL `CONSTRUCT` of
`?x a ?super` from `?x a/rdfs:subClassOf+ ?super`, loaded back with `load_rdf()`), target the subclasses explicitly, or
use pySHACL with RDFS inference (below).

## Recursive shapes

A shape can refer to itself, for example to say that everyone a person knows is a person who conforms to the same
shape. SHACL 1.0 leaves the meaning of such recursive shapes undefined, and processors differ. The validator gives them
*least-fixpoint* semantics: a node whose conformance depends on itself, through a cycle in the data, does not conform.
On data without cycles this changes nothing, but when Alice knows Bob and Bob knows Alice, both are reported, although
both have a name. The report then has a warning:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()
g.load_rdf(1, b"""
    @prefix ex: <http://example.org/> .
    ex:alice a ex:Person ; ex:name "Alice" ; ex:knows ex:bob .
    ex:bob   a ex:Person ; ex:name "Bob" .
""")
shapes = b"""
    @prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix ex: <http://example.org/> .
    ex:PersonShape sh:targetClass ex:Person ;
        sh:property [ sh:path ex:name ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:knows ; sh:node ex:PersonShape ] .
"""
print(g.validate_shacl(shapes)["conforms"])

g.load_rdf(2, b"<http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .")
report = g.validate_shacl(shapes)
for result in report["results"]:
    print(result["focus_node"], result["value"])
print(report["warnings"][0][:51])
```
///

!!! Output

    ```output
    True
    http://example.org/alice http://example.org/bob
    http://example.org/bob http://example.org/alice
    the shapes are recursive, and the validator gives r
    ```

The validator follows a recursive shape through the data by recursing itself, once per node: on a chain of many
thousands of nodes that each depend on the next (such as `ex:next` links checked with `sh:node` against the same
shape), its stack overflows, which ends the process.

## Literal values

The validator reads literals of some datatypes as values, and writes them back in a canonical form: booleans (`"1"`
becomes `"true"`), date-times (`"2020-01-01T00:00:00.000Z"` and `"2020-01-01T00:00:00+00:00"` become
`"2020-01-01T00:00:00Z"`), the integer types `xsd:long`, `xsd:short`, `xsd:byte`, their unsigned types and the
positive and negative integer types (`"042"^^xsd:long` becomes `"42"`, and a `xsd:short` even becomes an
`xsd:integer`), and language tags with a region (`@en-gb` becomes `@en-GB`). Literals of `xsd:integer`, `xsd:int`,
`xsd:decimal`, `xsd:double`, `xsd:float`, strings and other datatypes keep their exact form.

- In results, focus nodes and values are given back as written in the graph, so `node()` finds them. If two literals
  of the graph have the same canonical form (such as `"01"^^xsd:long` and `"001"^^xsd:long`), or the canonical form is
  itself a node, the canonical form is returned, which `node()` may not find.
- `sh:hasValue` and `sh:in` compare RDF terms, so a literal of theirs that is not in canonical form is rejected (see
  [supported SHACL](#supported-shacl)). A literal of `sh:targetNode` is looked up as written.
- `sh:disjoint` compares canonical forms, so it takes `"1"^^xsd:boolean` and `"true"^^xsd:boolean` to be the same
  value.

## Other limitations

- **No snapshot isolation.** As with SPARQL, validation reads the graph as it goes, holding no lock between triples,
  so other threads can write to the graph meanwhile; the result may or may not include their writes. Do not write to
  the graph while validating if you need a consistent result.
- **Performance.** Validation reads the triples it needs from the graph as it validates, and every `rdf:type` and
  `rdfs:subClassOf` triple of the view once, to index the classes. Most of the time is spent in the validator itself,
  which takes about as long as validating an in-memory copy of the same triples: a few seconds for 200,000 triples.
- **SHACL 1.2 reifier shapes see no reifiers.** The graph cannot hold triple terms, so `sh:reifierShape` has nothing
  to check, and `sh:reificationRequired true` is never satisfied.
- **Validation cannot be stopped.** There is no timeout. A validation releases the GIL while it runs, so other Python
  threads keep running.

## Using pySHACL instead

[pySHACL](https://github.com/RDFLib/pySHACL) is a SHACL validator for [rdflib](https://rdflib.readthedocs.io/) graphs.
It supports SHACL-SPARQL, SHACL Advanced Features and RDFS inference, so it is the better choice for shapes that use
them or for deep class hierarchies; it is slower, and works on a copy of the data in memory. Export the view you want to
validate with `to_rdf()`:

```{.python notest}
import pyshacl
import rdflib

data = rdflib.Graph().parse(data=g.snapshot_at(3).to_rdf(format="nt"), format="nt")
shapes_graph = rdflib.Graph().parse(data=shapes, format="turtle")
conforms, report_graph, report_text = pyshacl.validate(
    data, shacl_graph=shapes_graph, inference="rdfs"
)
print(conforms)
print(report_text)
```

To validate several times, export each snapshot:

```{.python notest}
for t in [1, 5, 7]:
    data = rdflib.Graph().parse(data=g.snapshot_at(t).to_rdf(format="nt"), format="nt")
    conforms, _, _ = pyshacl.validate(data, shacl_graph=shapes_graph, inference="rdfs")
    print(t, conforms)
```

pySHACL reports terms as RDF terms: the node `Alice` of a graph that was not loaded from RDF is
`raphtory:Alice`, as in `to_rdf()`.

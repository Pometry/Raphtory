"""
Builds a small temporal knowledge graph of people and companies, for trying the examples without
downloading anything.

    python scripts/build_people_graph.py GRAPH_PATH

Alice moves from Acme to Globex in September 2022, Bob from Globex to Initech in 2023 (holding both
jobs between January and July), and a CRM import in 2024 adds a second IRI for Alice with her email.
"""

import os
import shutil
import sys

from raphtory import PersistentGraph

P = b"@prefix ex: <http://example.org/> .\n"
HISTORY = [
    ("2021-01-04", "load", P + b"""
        ex:acme    a ex:Company ; ex:name "Acme Corp" .
        ex:globex  a ex:Company ; ex:name "Globex" .
        ex:initech a ex:Company ; ex:name "Initech" .
        ex:alice a ex:Person ; ex:name "Alice Smith" ; ex:email "alice.smith@example.com" ; ex:worksFor ex:acme .
        ex:bob   a ex:Person ; ex:name "Bob Jones"   ; ex:email "bob.jones@example.com"   ; ex:worksFor ex:globex .
        ex:carol a ex:Person ; ex:name "Carol White" ; ex:email "carol.white@example.com" ; ex:worksFor ex:acme ;
                 ex:manages ex:alice .
    """),
    ("2022-03-01", "load", P + b"""
        ex:dave a ex:Person ; ex:name "Dave Brown" ; ex:email "dave.brown@example.com" ; ex:worksFor ex:initech .
    """),
    ("2022-09-01", "retract", P + b"ex:alice ex:worksFor ex:acme . ex:carol ex:manages ex:alice ."),
    ("2022-09-01", "load", P + b"ex:alice ex:worksFor ex:globex ."),
    ("2023-01-01", "load", P + b"ex:bob ex:worksFor ex:initech ."),
    ("2023-07-01", "retract", P + b"ex:bob ex:worksFor ex:globex ."),
    ("2024-03-01", "load", P + b"""
        ex:crm-4711 a ex:Person ; ex:name "A. Smith" ; ex:email "alice.smith@example.com" .
    """),
    ("2025-02-01", "load", P + b"""
        ex:erin a ex:Person ; ex:name "Erin Green" ; ex:email "erin.green@example.com" ; ex:worksFor ex:acme .
        ex:carol ex:manages ex:erin .
    """),
]


def main(graph_path):
    graph = PersistentGraph()
    for date, action, document in HISTORY:
        (graph.load_rdf if action == "load" else graph.retract_rdf)(date, document)
    shutil.rmtree(graph_path, ignore_errors=True)
    os.makedirs(os.path.dirname(os.path.abspath(graph_path)), exist_ok=True)
    graph.save_to_file(graph_path)
    print(f"{graph.count_temporal_edges()} assertions and retractions -> {graph_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])

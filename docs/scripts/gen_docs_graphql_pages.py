import subprocess
from pathlib import Path

root = Path(__file__).parent.parent.parent
doc_root = Path("reference")
graphql_dir = doc_root / "reference" / "graphql"
schema_dir = root / "raphtory-graphql"

import os
print(os.getcwd())
print(os.listdir)

result = subprocess.run(
    [
        "npx", "graphql-markdown",
        "--no-toc",
        "--update-file", (graphql_dir / "graphql_API.md").as_posix(),
        (schema_dir / "schema.graphql").as_posix()
    ],
    capture_output=True,
    text=True
)
print(result)

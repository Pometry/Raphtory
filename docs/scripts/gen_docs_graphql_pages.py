import subprocess
from pathlib import Path
import mkdocs_gen_files

root = Path(__file__).parent.parent.parent
graphql_dir = root /"docs" / "reference" / "graphql"
schema_dir = root / "raphtory-graphql"

frontmatter = """---
hide:
  - navigation
---
"""

result = subprocess.run(
    [
        "npx", "graphql-markdown",
        "--no-toc",
        (schema_dir / "schema.graphql").as_posix()
    ],
    capture_output=True,
    text=True
)

output_str = result.stdout

doc_path = graphql_dir / "graphql_API.md"

with mkdocs_gen_files.open(doc_path, "w") as fd:
    print(f"{frontmatter}", file=fd)
    print(f"{output_str}", file=fd)

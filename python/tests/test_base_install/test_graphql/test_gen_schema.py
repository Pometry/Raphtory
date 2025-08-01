from raphtory.graphql import GraphServer, schema
from pathlib import Path

root = Path(__file__).parent.parent.parent.parent.parent
schema_dir = root / "raphtory-graphql"
graphql_file_path = (schema_dir / "schema.graphql").as_posix()

print(f"Creating graphql.schema file at {graphql_file_path}")

with open(graphql_file_path, "w", encoding="utf8") as f:
    f.write(schema())

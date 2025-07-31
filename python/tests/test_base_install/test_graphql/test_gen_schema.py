from raphtory.graphql import GraphServer, schema
from pathlib import Path

root = Path(__file__).parent
#graphql_dir = root /"docs" / "reference" / "graphql"
graphql_file_path = (root / "schema.graphql").as_posix()

print(f"Creating {graphql_file_path} graphql.schema file")

with open(graphql_file_path, 'w', encoding='utf8') as f:
    f.write(schema())


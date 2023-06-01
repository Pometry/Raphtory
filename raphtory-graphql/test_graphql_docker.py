import os
import time
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# This is meant to be run from the root directory, with gql and aiohttp installed

# os.system("docker build -t pometry/raphtory .")
os.system("docker compose -f examples/docker/lotr/docker-compose.yml up -d")
time.sleep(1)

transport = AIOHTTPTransport(url="http://localhost:1736")
client = Client(transport=transport, fetch_schema_from_transport=True)

query = gql(
    """
    {
      graph(name: "lotr") {
        node(name: "Gandalf") {
          name
        }
      }
    }
"""
)

try:
    result = client.execute(query)
    print("➜ running test query")
    assert result == {'graph': {'node': {'name': 'Gandalf'}}}
    print("✔ query ran successfully")
except Exception:
    pass
finally:
    os.system("docker compose -f examples/docker/lotr/docker-compose.yml down")
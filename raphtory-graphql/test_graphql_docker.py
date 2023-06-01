if __name__ == "__main__": # to prevent this from being executed by pytest
    import os
    import time
    from gql import gql, Client
    from gql.transport.aiohttp import AIOHTTPTransport

    # This is meant to be run from the root directory, with gql and aiohttp installed

    # os.system("docker build -t pometry/raphtory .")
    os.system("docker compose -f examples/docker/lotr/docker-compose.yml up -d")
    time.sleep(5)

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
        print("➜ running test query")
        result = client.execute(query)
        assert result == {'graph': {'node': {'name': 'Gandalf'}}}
        print("✔ query ran successfully")
    except Exception:
        raise
    finally:
        os.system("docker compose -f examples/docker/lotr/docker-compose.yml down")

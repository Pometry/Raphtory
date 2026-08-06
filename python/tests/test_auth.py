from time import time
import pytest
import jwt
import base64
import json
import requests
import tempfile
from datetime import datetime, timezone
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

# openssl genpkey -algorithm ed25519 -out raphtory-key.pem
# openssl pkey -in raphtory-key.pem -pubout -outform DER | base64
PUB_KEY = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
# cat raphtory-key.pem
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----"""

READ_JWT = jwt.encode({"access": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
READ_HEADERS = {
    "Authorization": f"Bearer {READ_JWT}",
}

WRITE_JWT = jwt.encode({"access": "rw"}, PRIVATE_KEY, algorithm="EdDSA")
WRITE_HEADERS = {
    "Authorization": f"Bearer {WRITE_JWT}",
}

# openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa-key.pem
# openssl pkey -in rsa-key.pem -pubout -outform DER | base64 | tr -d '\n'
RSA_PUB_KEY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4sqe3DlHB/DaSm8Ab99yKj0KDc/WZGFPwXeTbPwCMKKSEc8zuSuIZc/fHXLSORn1apMnDq3aLryfPwyNTbpvhGiYVyp76XQGwSlN+EF2TsJZVAzp4/EI+bnHeHyv2Yc5q6AkFtoBPNtAz2P/18g7Yv/eZqNNSd7FOeuRFRs9y0LkswvMelQmoMOK7UKdC00AyiGksvFvljNC70VT9b0uVHggJwUYT0hdCbdaDj2fCJZBEmTqBBr97u3fIHo5T41sIEEPgE2j368mI+uk6V1saEU1BU+hkcq56TabgVqUYZTln5Rdm1MuBsNz+NQwOmVxgPNo45H2cNwTfsPDAAESlwIDAQAB"
RSA_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDiyp7cOUcH8NpK
bwBv33IqPQoNz9ZkYU/Bd5Ns/AIwopIRzzO5K4hlz98dctI5GfVqkycOrdouvJ8/
DI1Num+EaJhXKnvpdAbBKU34QXZOwllUDOnj8Qj5ucd4fK/ZhzmroCQW2gE820DP
Y//XyDti/95mo01J3sU565EVGz3LQuSzC8x6VCagw4rtQp0LTQDKIaSy8W+WM0Lv
RVP1vS5UeCAnBRhPSF0Jt1oOPZ8IlkESZOoEGv3u7d8gejlPjWwgQQ+ATaPfryYj
66TpXWxoRTUFT6GRyrnpNpuBWpRhlOWflF2bUy4Gw3P41DA6ZXGA82jjkfZw3BN+
w8MAARKXAgMBAAECggEAWIH78nU2B97Syja8xGw/KUXODSreACnMDvRkKCXHkwR3
HhUvmeXn4tf3uo3rhhZf5TpNhViK7C93tIrpAHswd0u8nFP7rNW3px3ADJE7oywM
4ZTymJ8iQhdjRd3fYPT5qEWkn/hvgDkO94EOwT8nEhFKUeMMUDZs4RhSdBrACHk0
CrOC2S9xbgYb5OWGV6vkSqNB0k0Kv+LxU8sS46BLE7DxfpzSXDyeYaCAkk+wbwfb
hX7lysczbSl5l5Bulcf/LHL4Oa/5t+NcBZqyN6ylRXyqQ8LEdK4+TOJfvnePX1go
3rG4rtyaBCuW5JD1ytxUsyfh8WE4GinUbHWzxvaYQQKBgQD5PxF2CmqMY6yiaxU3
0LFtRS9DtwIPnPX3Wdchq7ivSU1W6sHJjNfyEggi10DSOOINalRM/ZnVlDo8hJ3A
SybESWWzLuDZNAAAWkmoir0UpnURz847tKd8hJUivhsbdQBeKwaCuepcW6Hdwzh1
JsJjXPovrzVGQe5FSRfBy7gswQKBgQDo78p/jEVHzuxHqSn3AsOdBdMZvPavpHb2
Bx7tRhZOOp2QiGUHZLfjI++sQQyTu1PJqmmxOOF+eD/zkqCkLLeZsmRYOQVDOQDM
Z+u+zKYRj7KaWBeGB2Oy/WEU0pGnhyMB/T5iHmroO0Hn4gDHqkEDvwFI7SUjLNAK
1RjTxVgdVwKBgCRHNMBspbOHcoI1eeIk4x5Xepitk4Q4QWjeT7zb5MbGsZYcF1bB
xFC8pSiFEi9HDkgLmPeX1gNLTuquFtP9XEgnssDQ6vNSaUmj2qLIhtrxm4qbJ5Zz
JgmutpJW/1UQw5vxQUJX0y/cOoQvvRD4MkUKLHQyWVu/jvHQwL95anZBAoGBAIrZ
9aGWYe3uINaOth8yHJzLTgz3oS0OIoOBtyPFNaKoOihfxalklmDlmQbbN74QWl/K
H3qu52vWDnkJHI0Awujxd/NG+iYaIqm2AMcZgpzRRavPeyY/3WRiua4J3x035txW
swsWCrAoMp8hD0n16Q9smj14bzzKh7ENWeFSr7W9AoGBAMOSyRdVQxVHXagh3fAa
+FNbR8pFmQC6bQGCO74DzGe6uKYpgu+XD1yinufwwsXxjieDXCHkKTGR92Kzp5VY
Hp6HhhhCcXICRRnbxhvdpyaDbCQrT522bqRJ4rNmSVYOQQiD2vng/HVB2oWMVwa+
fEtYNjbxjhX9qInHjHxeaNOp
-----END PRIVATE KEY-----"""

NEW_TEST_GRAPH = """mutation { newGraph(path:"test", graphType:EVENT) }"""

QUERY_NAMESPACES = """query { namespaces { list{ path} } }"""
QUERY_ROOT = """query { root { graphs { list{ path }} } }"""
QUERY_GRAPH = """query { graph(path: "test") { path } }"""
TEST_QUERIES = [QUERY_NAMESPACES, QUERY_GRAPH, QUERY_ROOT]


def raphtory_url(port: int) -> str:
    return f"http://localhost:{port}"


def assert_successful_response(response: requests.Response):
    assert "errors" not in response.json()
    assert type(response.json()["data"]) == dict
    assert len(response.json()["data"]) == 1


# TODO: implement this so we can use the with sintax
def add_test_graph(port):
    requests.post(
        raphtory_url(port),
        headers=WRITE_HEADERS,
        data=json.dumps({"query": NEW_TEST_GRAPH}),
    )


def test_expired_token():
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        exp = time() - 100
        token = jwt.encode({"access": "ro", "exp": exp}, PRIVATE_KEY, algorithm="EdDSA")
        headers = {
            "Authorization": f"Bearer {token}",
        }
        response = requests.post(
            raphtory_url(port), headers=headers, data=json.dumps({"query": QUERY_ROOT})
        )
        assert response.status_code == 401

        token = jwt.encode({"access": "rw", "exp": exp}, PRIVATE_KEY, algorithm="EdDSA")
        headers = {
            "Authorization": f"Bearer {token}",
        }
        response = requests.post(
            raphtory_url(port), headers=headers, data=json.dumps({"query": QUERY_ROOT})
        )
        assert response.status_code == 401


def test_not_yet_valid_token_rejected():
    """A token whose `nbf` (not-before) is in the future must be rejected now."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        nbf = time() + 3600  # only becomes valid an hour from now
        for access in ("ro", "rw"):
            token = jwt.encode(
                {"access": access, "nbf": nbf}, PRIVATE_KEY, algorithm="EdDSA"
            )
            headers = {
                "Authorization": f"Bearer {token}",
            }
            response = requests.post(
                raphtory_url(port),
                headers=headers,
                data=json.dumps({"query": QUERY_ROOT}),
            )
            assert response.status_code == 401


def test_already_valid_nbf_token_accepted():
    """A token whose `nbf` is in the past is honoured normally (nbf validation must
    not reject already-valid tokens)."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        nbf = time() - 3600  # became valid an hour ago
        token = jwt.encode({"access": "ro", "nbf": nbf}, PRIVATE_KEY, algorithm="EdDSA")
        headers = {
            "Authorization": f"Bearer {token}",
        }
        response = requests.post(
            raphtory_url(port), headers=headers, data=json.dumps({"query": QUERY_ROOT})
        )
        assert_successful_response(response)


def test_nbf_and_exp_window():
    """Combine `nbf` and `exp` to define a validity *window* and check the server
    honours both ends. The default 60s clock-skew leeway applies to both claims."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        now = int(time())

        def read_with(claims):
            token = jwt.encode(claims, PRIVATE_KEY, algorithm="EdDSA")
            return requests.post(
                raphtory_url(port),
                headers={"Authorization": f"Bearer {token}"},
                data=json.dumps({"query": QUERY_ROOT}),
            )

        # Valid for one minute, starting 5s from now: the 5s start is inside the 60s
        # leeway, so the token is already valid and its exp (65s out) hasn't passed.
        assert_successful_response(
            read_with({"access": "ro", "nbf": now + 5, "exp": now + 65})
        )

        # Same one-minute window, placed an hour ahead (well beyond leeway):
        # not yet valid -> rejected.
        assert (
            read_with(
                {"access": "ro", "nbf": now + 3600, "exp": now + 3660}
            ).status_code
            == 401
        )

        # Same one-minute window, but it closed over a minute ago (beyond leeway):
        # expired -> rejected.
        assert (
            read_with({"access": "ro", "nbf": now - 120, "exp": now - 65}).status_code
            == 401
        )


@pytest.mark.parametrize("query", TEST_QUERIES)
def test_default_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        add_test_graph(port)
        data = json.dumps({"query": query})

        response = requests.post(raphtory_url(port), data=data)
        assert response.status_code == 401

        response = requests.post(raphtory_url(port), headers=READ_HEADERS, data=data)
        assert_successful_response(response)

        response = requests.post(raphtory_url(port), headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


@pytest.mark.parametrize("query", TEST_QUERIES)
def test_disabled_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir,
        config={"auth": {"public_key": PUB_KEY, "require_auth_for_reads": False}},
    ).start() as server:
        port = server.port()
        add_test_graph(port)
        data = json.dumps({"query": query})

        response = requests.post(raphtory_url(port), data=data)
        assert_successful_response(response)

        response = requests.post(raphtory_url(port), headers=READ_HEADERS, data=data)
        assert_successful_response(response)

        response = requests.post(raphtory_url(port), headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


ADD_NODE = """
query {
  updateGraph(path: "test") {
    addNode(time: 0, name: "node") {
      success
    }
  }
}
"""
ADD_EDGE = """
query {
  updateGraph(path: "test") {
    addNode(time: 0, name: "node") {
      success
    }
  }
}
"""
ADD_TEMP_PROP = """
query {
  updateGraph(path: "test") {
    addProperties(t: 0, properties: [{key: "value", value: {str: "value"}}])
  }
}
"""
ADD_CONST_PROP = """
query {
  updateGraph(path: "test") {
    addMetadata(properties: [{key: "value", value: {str: "value"}}])
  }
}
"""


@pytest.mark.parametrize("query", [ADD_NODE, ADD_EDGE, ADD_TEMP_PROP, ADD_CONST_PROP])
def test_update_graph(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        add_test_graph(port)
        data = json.dumps({"query": query})

        response = requests.post(raphtory_url(port), data=data)
        assert response.status_code == 401

        response = requests.post(raphtory_url(port), headers=READ_HEADERS, data=data)
        assert response.json()["data"] is None
        assert (
            response.json()["errors"][0]["message"]
            == "The requested endpoint requires write access"
        )

        response = requests.post(raphtory_url(port), headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


NEW_GRAPH = """mutation { newGraph(path:"new", graphType:EVENT) }"""
MOVE_GRAPH = """mutation { moveGraph(path:"test", newPath:"moved") }"""
COPY_GRAPH = """mutation { copyGraph(path:"test", newPath:"copied") }"""
DELETE_GRAPH = """mutation { deleteGraph(path:"test") }"""
CREATE_SUBGRAPH = """mutation { createSubgraph(parentPath:"test", newPath: "subgraph", nodes: [], overwrite: false) }"""


@pytest.mark.parametrize(
    "query", [NEW_GRAPH, MOVE_GRAPH, COPY_GRAPH, DELETE_GRAPH, CREATE_SUBGRAPH]
)
def test_mutations(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        add_test_graph(port)
        data = json.dumps({"query": query})

        response = requests.post(raphtory_url(port), data=data)
        assert response.status_code == 401

        response = requests.post(raphtory_url(port), headers=READ_HEADERS, data=data)
        assert response.json()["data"] is None
        assert (
            response.json()["errors"][0]["message"]
            == "The requested endpoint requires write access"
        )

        response = requests.post(raphtory_url(port), headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


def test_raphtory_client():
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        client = RaphtoryClient(url=raphtory_url(port), token=WRITE_JWT)
        client.new_graph("test", "EVENT")
        g = client.remote_graph("test")
        g.add_node(0, "test")
        node = g.node("test")
        g = client.receive_graph("test")
        assert g.node("test") is not None


def test_raphtory_client_write_denied_for_read_jwt():
    """RaphtoryClient initialized with a read JWT is denied write operations."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        client = RaphtoryClient(url=raphtory_url(port), token=READ_JWT)
        with pytest.raises(Exception, match="requires write access"):
            client.new_graph("test", "EVENT")


# --- RSA JWT support ---


def test_rsa_signed_jwt_rs256_accepted():
    """Server configured with an RSA public key accepts RS256-signed JWTs."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": RSA_PUB_KEY}}
    ).start() as server:
        port = server.port()
        token = jwt.encode({"access": "ro"}, RSA_PRIVATE_KEY, algorithm="RS256")
        response = requests.post(
            raphtory_url(port),
            headers={"Authorization": f"Bearer {token}"},
            data=json.dumps({"query": QUERY_ROOT}),
        )
        assert_successful_response(response)


def test_rsa_signed_jwt_rs512_accepted():
    """RS512 JWT is also accepted for the same RSA key (different hash, same key material)."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": RSA_PUB_KEY}}
    ).start() as server:
        port = server.port()
        token = jwt.encode({"access": "ro"}, RSA_PRIVATE_KEY, algorithm="RS512")
        response = requests.post(
            raphtory_url(port),
            headers={"Authorization": f"Bearer {token}"},
            data=json.dumps({"query": QUERY_ROOT}),
        )
        assert_successful_response(response)


def test_eddsa_jwt_rejected_against_rsa_key():
    """EdDSA JWT is rejected when the server is configured with an RSA public key."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": RSA_PUB_KEY}}
    ).start() as server:
        port = server.port()
        token = jwt.encode({"access": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
        response = requests.post(
            raphtory_url(port),
            headers={"Authorization": f"Bearer {token}"},
            data=json.dumps({"query": QUERY_ROOT}),
        )
        assert response.status_code == 401


def test_raphtory_client_read_jwt_can_receive_graph():
    """RaphtoryClient initialized with a read JWT can download graphs."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        client = RaphtoryClient(url=raphtory_url(port), token=WRITE_JWT)
        client.new_graph("test", "EVENT")
        client.remote_graph("test").add_node(0, "mynode")

        client2 = RaphtoryClient(url=raphtory_url(port), token=READ_JWT)
        g = client2.receive_graph("test")
        assert g.node("mynode") is not None


def test_upload_graph():
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, config={"auth": {"public_key": PUB_KEY}}
    ).start() as server:
        port = server.port()
        client = RaphtoryClient(url=raphtory_url(port), token=WRITE_JWT)
        g = Graph()
        g.add_node(0, "uploaded-node")
        tmp_dir = tempfile.mkdtemp()
        path = tmp_dir + "/graph"
        g.save_to_zip(path)
        client.upload_graph(path="uploaded", file_path=path)
        g = client.receive_graph("uploaded")
        assert g.node("uploaded-node") is not None

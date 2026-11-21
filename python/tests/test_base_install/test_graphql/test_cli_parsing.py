import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def get_config_file():
    cfg = tempfile.NamedTemporaryFile(mode="w+", suffix=".toml", delete=False)
    cfg.write("[cache]\ncapacity = 123\n")
    cfg.flush()
    return cfg


def test_raphtory_server_no_arguments():
    env = os.environ.copy()
    env.pop("RAPHTORY_CACHE_CAPACITY", None)

    result = subprocess.run(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--print-config",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    app_config = json.loads(result.stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 30


def test_raphtory_server_with_config_file():
    env = os.environ.copy()
    env.pop("RAPHTORY_CACHE_CAPACITY", None)
    config_file = get_config_file()

    result = subprocess.run(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--config-file",
            config_file.name,
            "--print-config",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    app_config = json.loads(result.stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 123


def test_raphtory_server_with_env_variable():
    env = os.environ.copy()
    env["RAPHTORY_CACHE_CAPACITY"] = "456"
    config_file = get_config_file()

    result = subprocess.run(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--config-file",
            config_file.name,
            "--print-config",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    app_config = json.loads(result.stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 456


def test_raphtory_server_with_command_line_argument():
    env = os.environ.copy()
    env["RAPHTORY_CACHE_CAPACITY"] = "456"
    config_file = get_config_file()

    result = subprocess.run(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--config-file",
            config_file.name,
            "--cache-capacity",
            "789",
            "--print-config",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    app_config = json.loads(result.stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 789


def test_sparql_limits_from_every_config_source():
    env = {k: v for k, v in os.environ.items() if not k.startswith("RAPHTORY_")}
    env["RAPHTORY_MAX_SPARQL_TRIPLE_PATTERNS"] = "5"
    env["RAPHTORY_MAX_SHACL_VALIDATION_MS"] = "3000"
    with tempfile.TemporaryDirectory() as tmp:
        config_file = Path(tmp) / "config.toml"
        config_file.write_text(
            "[concurrency]\nmax_sparql_query_length = 100\nmax_shacl_results = 7\n"
        )
        printed = subprocess.run(
            [
                Path(sys.executable).with_name("raphtory"),
                "server",
                "--work-dir",
                Path(tmp) / "graphs",
                "--config-file",
                config_file,
                "--max-sparql-result-bytes",
                "1000",
                "--max-sparql-query-ms",
                "2000",
                "--max-shacl-shapes-length",
                "2000",
                "--print-config",
            ],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
    concurrency = json.loads(printed.stdout)["concurrency"]
    assert concurrency["max_sparql_query_length"] == 100
    assert concurrency["max_sparql_triple_patterns"] == 5
    assert concurrency["max_sparql_result_bytes"] == 1000
    assert concurrency["max_sparql_query_ms"] == 2000
    assert concurrency["max_shacl_shapes_length"] == 2000
    assert concurrency["max_shacl_results"] == 7
    assert concurrency["max_shacl_validation_ms"] == 3000

import json
import os
import subprocess
import tempfile


def get_app_config(stdout: str) -> dict:
    server_line = next(
        (line for line in stdout.splitlines() if "Server configurations:" in line),
        None,
    )
    _, _, serialized = server_line.partition("Server configurations: ")
    json_start = serialized.find("{")
    json_end = serialized.rfind("}")
    json_str = serialized[json_start : json_end + 1]
    server_config_json = json.loads(json_str)
    return server_config_json["config"]


def get_config_file():
    cfg = tempfile.NamedTemporaryFile(mode="w+", suffix=".toml", delete=False)
    cfg.write("[cache]\ncapacity = 123\n")
    cfg.flush()
    return cfg


def test_raphtory_server_no_arguments():
    env = os.environ.copy()
    env.pop("RAPHTORY_CACHE_CAPACITY", None)

    process = subprocess.Popen(
        ["raphtory", "server", "--port", "1737", "--log-level", "debug"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.terminate()

    stdout, stderr = process.communicate(timeout=1)
    app_config = get_app_config(stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 30


def test_raphtory_server_with_config_file():
    env = os.environ.copy()
    env.pop("RAPHTORY_CACHE_CAPACITY", None)
    config_file = get_config_file()

    process = subprocess.Popen(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--log-level",
            "debug",
            "--config-file",
            config_file.name,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.terminate()

    stdout, stderr = process.communicate(timeout=1)
    app_config = get_app_config(stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 123


def test_raphtory_server_with_env_variable():
    env = os.environ.copy()
    env["RAPHTORY_CACHE_CAPACITY"] = "456"
    config_file = get_config_file()

    process = subprocess.Popen(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--log-level",
            "debug",
            "--config-file",
            config_file.name,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.terminate()

    stdout, stderr = process.communicate(timeout=1)
    app_config = get_app_config(stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 456


def test_raphtory_server_with_command_line_argument():
    env = os.environ.copy()
    env["RAPHTORY_CACHE_CAPACITY"] = "456"
    config_file = get_config_file()

    process = subprocess.Popen(
        [
            "raphtory",
            "server",
            "--port",
            "1737",
            "--log-level",
            "debug",
            "--config-file",
            config_file.name,
            "--cache-capacity",
            "789",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.terminate()

    stdout, stderr = process.communicate(timeout=1)
    app_config = get_app_config(stdout)
    cache_capacity = app_config["cache"]["capacity"]
    assert cache_capacity == 789

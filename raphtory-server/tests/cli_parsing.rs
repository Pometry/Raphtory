use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};

use raphtory_graphql::config::{app_config::AppConfig, cache_config::DEFAULT_CACHE_CAPACITY};
use serde::Deserialize;
use serde_json::Value;
use tempfile::Builder;

fn server_bin() -> std::path::PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_raphtory-server")
        .or_else(|| std::env::var_os("CARGO_BIN_EXE_raphtory-server"))
        .map(std::path::PathBuf::from)
        .expect(
            "failed to locate raphtory-server binary via NEXTEST_BIN_EXE_raphtory-server or CARGO_BIN_EXE_raphtory-server",
        )
}

fn get_app_config(stdout: String) -> AppConfig {
    let server_config_serialized = stdout
        .lines()
        .find(|line| line.contains("Server configurations:"))
        .expect("failed to find app config in CLI output")
        .split_once("Server configurations: ")
        .expect("failed to parse app config from CLI output")
        .1;
    let json_start = server_config_serialized.find('{').expect("no JSON found");
    let json_end = server_config_serialized
        .rfind('}')
        .expect("no JSON end found")
        + 1;
    let json_str = &server_config_serialized[json_start..json_end];

    let server_config_json: Value =
        serde_json::from_str(json_str).expect("failed to parse config JSON");
    let config = &server_config_json["config"];
    AppConfig::deserialize(config).expect("failed to deserialize AppConfig")
}

fn config_file() -> tempfile::NamedTempFile {
    let mut config_file = Builder::new()
        .suffix(".toml")
        .tempfile()
        .expect("failed to create temporary config file for CLI test");
    write!(config_file, "[cache]\ncapacity = 123\n")
        .expect("failed to write temporary cache config");
    config_file
        .flush()
        .expect("failed to flush temporary cache config");
    config_file
}

#[test]
fn test_cli_parsing_no_arguments() {
    let server_bin = server_bin();

    let mut child = Command::new(server_bin)
        .args(["server", "--log-level", "debug"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("RAPHTORY_CACHE_CAPACITY")
        .spawn()
        .expect("failed to spawn raphtory-server CLI");

    std::thread::sleep(Duration::from_secs(5));
    child.kill().expect("failed to kill raphtory-server CLI");
    let output = child
        .wait_with_output()
        .expect("failed to collect raphtory-server CLI output");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let config = get_app_config(stdout.clone());
    let cache_capacity = config.cache.capacity;
    assert_eq!(cache_capacity, DEFAULT_CACHE_CAPACITY);
}

#[test]
fn test_cli_parsing_with_config_file() {
    let config_file = config_file();

    let server_bin = server_bin();

    let mut child = Command::new(server_bin)
        .args([
            "server",
            "--log-level",
            "debug",
            "--config-file",
            config_file.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("RAPHTORY_CACHE_CAPACITY")
        .spawn()
        .expect("failed to spawn raphtory-server CLI");

    std::thread::sleep(Duration::from_secs(5));
    child.kill().expect("failed to kill raphtory-server CLI");
    let output = child
        .wait_with_output()
        .expect("failed to collect raphtory-server CLI output");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let config = get_app_config(stdout.clone());
    let cache_capacity = config.cache.capacity;
    assert_eq!(cache_capacity, 123);
}

#[test]
fn test_cli_parsing_with_env_variable() {
    let config_file = config_file();

    let server_bin = server_bin();

    let mut child = Command::new(server_bin)
        .args([
            "server",
            "--log-level",
            "debug",
            "--config-file",
            config_file.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("RAPHTORY_CACHE_CAPACITY", "456")
        .spawn()
        .expect("failed to spawn raphtory-server CLI");

    std::thread::sleep(Duration::from_secs(5));
    child.kill().expect("failed to kill raphtory-server CLI");
    let output = child
        .wait_with_output()
        .expect("failed to collect raphtory-server CLI output");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let config = get_app_config(stdout.clone());
    let cache_capacity = config.cache.capacity;
    assert_eq!(cache_capacity, 456);
}

#[test]
fn test_cli_parsing_with_server_argument() {
    let config_file = config_file();

    let server_bin = server_bin();

    let mut child = Command::new(server_bin)
        .args([
            "server",
            "--log-level",
            "debug",
            "--config-file",
            config_file.path().to_str().unwrap(),
            "--cache-capacity",
            "789",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("RAPHTORY_CACHE_CAPACITY", "456")
        .spawn()
        .expect("failed to spawn raphtory-server CLI");

    std::thread::sleep(Duration::from_secs(5));
    child.kill().expect("failed to kill raphtory-server CLI");
    let output = child
        .wait_with_output()
        .expect("failed to collect raphtory-server CLI output");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let config = get_app_config(stdout.clone());
    let cache_capacity = config.cache.capacity;
    assert_eq!(cache_capacity, 789);
}

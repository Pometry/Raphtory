use clap::{CommandFactory, Parser};
use dynamic_graphql::{
    internal::Registry, Context, ExpandObject, ExpandObjectFields, Request, Result,
};
use raphtory::prelude::Config;
use raphtory_graphql::{
    cli::Commands,
    config::app_config::AppConfigBuilder,
    model::QueryRoot,
    plugin::{
        schema::RegisterPlugin,
        server::{extension::ServerExtension, plugin::ServerPlugin},
    },
    register_cli_plugin,
    server::ServerError,
    GraphServer,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_value, Value};
use std::{io::Write, path::PathBuf};
use tempfile::NamedTempFile;

#[derive(clap::Args, Debug, Serialize, Deserialize, Default, Clone)]
struct TestArgs {
    #[arg(long)]
    test: Option<String>,
}

impl ServerExtension for TestArgs {
    fn apply(&self, server: GraphServer) -> Result<GraphServer, ServerError> {
        match self.test.clone() {
            None => Ok(server),
            Some(test_value) => {
                let test_value = TestValue(test_value);
                Ok(server
                    .with_schema_data(test_value)
                    .with_schema_plugin(TestSchemaPlugin))
            }
        }
    }

    fn name(&self) -> &str {
        "test"
    }

    fn update_from_json(&mut self, value: &Value) -> std::result::Result<(), ServerError> {
        // only update if test is not already specified via command-line
        if self.test.is_none() {
            *self = Self::deserialize(value).map_err(ServerError::config_error)?;
        }
        Ok(())
    }

    fn to_json(&self) -> std::result::Result<Value, ServerError> {
        to_value(self).map_err(ServerError::config_error)
    }
}

struct TestValue(String);

#[derive(ExpandObject)]
struct TestQuery<'a>(&'a QueryRoot);

#[ExpandObjectFields]
impl<'a> TestQuery<'a> {
    fn test(ctx: &Context) -> Result<String> {
        let v = ctx.data::<TestValue>()?;
        Ok(v.0.clone())
    }
}

#[derive(Clone)]
struct TestSchemaPlugin;

impl RegisterPlugin for TestSchemaPlugin {
    fn register(&self, registry: Registry) -> Registry {
        registry.register::<TestQuery>()
    }
}

#[derive(Copy, Clone)]
struct TestArgPlugin;
impl ServerPlugin for TestArgPlugin {
    type Extension = TestArgs;
    fn new(&self) -> Self::Extension {
        TestArgs::default()
    }
}

register_cli_plugin!(TestArgPlugin);

#[tokio::test]
async fn test_cli_parsing_extension() {
    let mut cmd = raphtory_graphql::cli::Args::command();

    // make sure the `--test` option appears in the help for server arguments
    let mut help_buffer = Vec::new();
    cmd.find_subcommand_mut("server")
        .unwrap()
        .write_help(&mut help_buffer)
        .unwrap();
    assert!(str::from_utf8(&help_buffer).unwrap().contains("--test"));

    // check the processing works
    let args_input: Vec<&str> = vec![r"raphtory-server", "server", "--test", "test"];
    let args = raphtory_graphql::cli::Args::try_parse_from(args_input).unwrap();
    let server = match args.command {
        Commands::Server(server_args) => GraphServer::new_from_args(server_args).await.unwrap(),
        Commands::Schema => {
            panic!("expected server args")
        }
    };

    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    assert_eq!(result.errors, vec![]);
    assert_eq!(result.data.into_json().unwrap(), json!({ "test": "test"}));

    // check the plugins are local so a server without the test argument doesn't have the query
    let args_input: Vec<&str> = vec![r"raphtory-server", "server"];
    let args = raphtory_graphql::cli::Args::try_parse_from(args_input).unwrap();
    let server = match args.command {
        Commands::Server(server_args) => GraphServer::new_from_args(server_args).await.unwrap(),
        Commands::Schema => {
            panic!("expected server args")
        }
    };

    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    let error = result.errors.first().unwrap();
    assert_eq!(
        error.message,
        "Unknown field \"test\" on type \"QueryRoot\"."
    );
}

#[tokio::test]
async fn test_extension_via_conf() {
    let args = AppConfigBuilder::new()
        .with_extension(TestArgs {
            test: Some("test2".to_string()),
        })
        .build();
    let server = GraphServer::new(PathBuf::new(), Some(args), Config::default())
        .await
        .unwrap();
    // check the processing works
    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    assert_eq!(result.errors, vec![]);
    assert_eq!(result.data.into_json().unwrap(), json!({ "test": "test2"}));

    // check the plugins are local so a server without the test argument doesn't have the query
    let server = GraphServer::new(PathBuf::new(), None, Config::default())
        .await
        .unwrap();

    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    let error = result.errors.first().unwrap();
    assert_eq!(
        error.message,
        "Unknown field \"test\" on type \"QueryRoot\"."
    );
}

#[tokio::test]
async fn test_config_file_cmd_override() {
    let mut config_file = NamedTempFile::with_suffix(".toml").unwrap();
    write!(
        &mut config_file,
        r#"
    [extensions.test]
    test = "test_from_file"
    "#
    )
    .unwrap();
    config_file.flush().unwrap();
    // check the processing works
    let args_input: Vec<&str> = vec![
        r"raphtory-server",
        "server",
        "--test",
        "test",
        "--config-file",
        config_file.path().to_str().unwrap(),
    ];
    let args = raphtory_graphql::cli::Args::try_parse_from(args_input).unwrap();
    let server = match args.command {
        Commands::Server(server_args) => GraphServer::new_from_args(server_args).await.unwrap(),
        Commands::Schema => {
            panic!("expected server args")
        }
    };

    // check the processing works
    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    assert_eq!(result.errors, vec![]);
    assert_eq!(result.data.into_json().unwrap(), json!({ "test": "test"}));
}

#[tokio::test]
async fn test_config_file() {
    let mut config_file = NamedTempFile::with_suffix(".toml").unwrap();
    write!(
        &mut config_file,
        r#"
    [extensions.test]
    test = "test_from_file"
    "#
    )
    .unwrap();
    config_file.flush().unwrap();
    // check the processing works
    let args_input: Vec<&str> = vec![
        r"raphtory-server",
        "server",
        "--config-file",
        config_file.path().to_str().unwrap(),
    ];
    let args = raphtory_graphql::cli::Args::try_parse_from(args_input).unwrap();
    let server = match args.command {
        Commands::Server(server_args) => GraphServer::new_from_args(server_args).await.unwrap(),
        Commands::Schema => {
            panic!("expected server args")
        }
    };

    // check the processing works
    let schema = server.build_schema(None).await.unwrap();
    let query = r"{ test }";
    let request = Request::new(query);
    let result = schema.execute(request).await;
    assert_eq!(result.errors, vec![]);
    assert_eq!(
        result.data.into_json().unwrap(),
        json!({ "test": "test_from_file"})
    );
}

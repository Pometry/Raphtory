use clap::{ArgMatches, Args, Command, CommandFactory, Error, FromArgMatches, Parser};
use dynamic_graphql::{
    internal::Registry, Context, ExpandObject, ExpandObjectFields, Request, Result,
};
use raphtory::prelude::Config;
use raphtory_graphql::{
    cli::{register_cli_plugin, ArgumentExtension, ArgumentExtensionPlugin, Commands},
    config::app_config::AppConfigBuilder,
    model::{plugins::query_plugin::RegisterPlugin, QueryRoot},
    server::ServerError,
    GraphServer,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;

#[derive(clap::Args, Debug, Serialize, Deserialize, Default, Clone)]
struct TestArgs {
    #[arg(long)]
    test: Option<String>,
}

impl ArgumentExtension for TestArgs {
    fn process_args(&self, server: GraphServer) -> Result<GraphServer, ServerError> {
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

struct TestSchemaPlugin;

impl RegisterPlugin for TestSchemaPlugin {
    fn register(&self, registry: Registry) -> Registry {
        registry.register::<TestQuery>()
    }
}

struct TestArgPlugin;
impl ArgumentExtensionPlugin for TestArgPlugin {
    type Extension = TestArgs;
    fn new_args(&self) -> Self::Extension {
        TestArgs::default()
    }
}

#[tokio::test]
async fn test_cli_parsing_extension() {
    register_cli_plugin(TestArgPlugin);
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
async fn test_config_file_support() {
    register_cli_plugin(TestArgPlugin);
    let mut cmd = raphtory_graphql::cli::Args::command();

    // check the processing works
    let args_input: Vec<&str> = vec![r"raphtory-server", "server", "--test", "test"];
    let args = raphtory_graphql::cli::Args::try_parse_from(args_input).unwrap();
    let config = match args.command {
        Commands::Server(server_args) => AppConfigBuilder::new()
            .update_from_args(&server_args)
            .unwrap()
            .build(),
        Commands::Schema => {
            panic!("expected server args")
        }
    };

    let json_config = json!(config);

    println!("{}", json_config);
    assert_eq!(json_config["extensions"]["test"]["test"], json!("test"));
}

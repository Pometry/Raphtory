mod cli;

#[tokio::main]
async fn main() -> IoResult<()> {
    cli::cli()
}

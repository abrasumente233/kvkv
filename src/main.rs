use clap::Parser;
use std::error::Error;
use tracing::info;

mod backend;
mod command;
mod coordinator;
mod kvstore;
mod participant;
mod proto;
mod resp;
mod trace;

#[derive(Parser)]
struct Cli {
    /// Run as coordinator, and run as participant if unspecified
    #[clap(short)]
    coordinator: bool,

    /// Port number
    #[clap(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    trace::init()?;

    let cli = Cli::parse();

    info!("Hello from kvkv []~（￣▽￣）~*");
    if cli.coordinator {
        coordinator::run(cli.port).await.unwrap();
    } else {
        participant::run(cli.port).await.unwrap();
    }

    Ok(())
}

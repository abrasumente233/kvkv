use clap::Parser;
use std::error::Error;
use tracing::info;

mod backend;
mod command;
mod coordinator;
mod kvstore;
mod participant;
mod resp;
mod trace;

#[derive(Parser)]
struct Cli {
    /// Run as coordinator, and run as participant if unspecified
    #[clap(short)]
    coordinator: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    trace::init()?;

    let cli = Cli::parse();

    info!("Hello from kvkv []~（￣▽￣）~*");
    if cli.coordinator {
        coordinator::run().await;
    } else {
        participant::run().await;
    }

    Ok(())
}

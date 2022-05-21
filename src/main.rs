use clap::Parser;
use std::error::Error;
use tracing::info;

mod backend;
mod command;
mod map;
mod master;
mod proto;
mod replica;
mod resp;
mod trace;

#[derive(Parser)]
struct Cli {
    /// Listen on this port
    #[clap(short, long)]
    port: u16,

    /// Replica addresses. 
    /// If specified, the program will run as master and try to connect to the
    /// replicas.
    #[clap(short, long)]
    replica_addresses: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    trace::init()?;

    let cli = Cli::parse();

    info!("hello from kvkv []~（￣▽￣）~*");
    if cli.replica_addresses.len() != 0 {
        master::run(cli.port, cli.replica_addresses).await.unwrap();
    } else {
        replica::run(cli.port).await.unwrap();
    }

    Ok(())
}

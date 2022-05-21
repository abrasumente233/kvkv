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
    /// Run as coordinator, and run as participant if unspecified
    #[clap(short)]
    master: bool,

    /// Port number
    #[clap(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    trace::init()?;

    let cli = Cli::parse();

    info!("hello from kvkv []~（￣▽￣）~*");
    if cli.master {
        master::run(cli.port).await.unwrap();
    } else {
        replica::run(cli.port).await.unwrap();
    }

    Ok(())
}

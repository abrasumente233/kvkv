use std::error::Error;

use tracing::info;

pub async fn run() {
    info!("Starting the coordinator");
    run_coordinator().await.unwrap();
}

async fn run_coordinator() -> Result<(), Box<dyn Error>> {
    Ok(())
}
